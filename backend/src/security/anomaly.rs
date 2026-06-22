//! Garde-fous de sécurité : résolution de l'IP cliente et détection d'anomalies.
//!
//! Hypothèse d'audit (cahier §7.1) : toute requête entrante peut être falsifiée. Ces défenses
//! sont purement serveur-side.

use std::net::IpAddr;
use std::sync::Mutex;
use std::time::Instant;

use axum::http::HeaderMap;
use dashmap::DashMap;

use crate::config::AnomalyConfig;

/// Résout l'IP cliente.
///
/// Par défaut, on utilise l'adresse du pair TCP (non falsifiable au niveau réseau). Les
/// en-têtes `X-Forwarded-For` ne sont pris en compte que si `trust_forwarded` est activé —
/// à n'activer que derrière **exactement un** reverse-proxy de confiance (sinon un client
/// peut forger l'en-tête). On retient alors l'entrée la plus à droite (celle ajoutée par le
/// proxy de confiance).
pub fn client_ip(headers: &HeaderMap, peer: IpAddr, trust_forwarded: bool) -> IpAddr {
    if !trust_forwarded {
        return peer;
    }
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        // **Strictement** l'entrée la plus à droite (celle ajoutée par le proxy de confiance).
        // On ne saute PAS un dernier hop malformé pour retomber sur une entrée contrôlable par le
        // client : si le hop de droite est illisible, on ignore tout l'en-tête (S-11).
        if let Some(last) = xff.rsplit(',').next() {
            if let Ok(ip) = last.trim().parse::<IpAddr>() {
                return ip;
            }
        }
    }
    if let Some(real) = headers
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<IpAddr>().ok())
    {
        return real;
    }
    peer
}

struct BadWindow {
    count: u32,
    window_start: Instant,
}

/// Détecte les IP au comportement manifestement anormal (rafales de requêtes invalides)
/// et les bannit temporairement (cahier §7.4).
pub struct AnomalyGuard {
    cfg: AnomalyConfig,
    bad: DashMap<IpAddr, Mutex<BadWindow>>,
    banned: DashMap<IpAddr, Instant>, // valeur = instant de fin de bannissement
}

impl AnomalyGuard {
    pub fn new(cfg: AnomalyConfig) -> Self {
        Self {
            cfg,
            bad: DashMap::new(),
            banned: DashMap::new(),
        }
    }

    /// Vrai si l'IP est actuellement bannie (nettoie l'entrée si le ban a expiré).
    pub fn is_banned(&self, ip: IpAddr, now: Instant) -> bool {
        // Un pair inconnu (UNSPECIFIED, p.ex. gRPC sans `remote_addr`) n'est jamais « banni » :
        // sinon un seul mauvais client bannirait 0.0.0.0 et donc tous ses semblables (S-9).
        if ip.is_unspecified() {
            return false;
        }
        match self.banned.get(&ip).map(|e| *e.value()) {
            Some(until) if until > now => true,
            Some(_) => {
                self.banned.remove(&ip);
                false
            }
            None => false,
        }
    }

    /// Enregistre une requête « mauvaise » (400/401/429). Bannit si le seuil est franchi.
    pub fn record_bad(&self, ip: IpAddr, now: Instant) {
        // Pas de suivi pour un pair inconnu (cf. `is_banned`) : éviter d'empoisonner 0.0.0.0 (S-9).
        if ip.is_unspecified() {
            return;
        }
        // Garantit une place pour `ip` dans la map des fenêtres : sinon un abuseur inédit ne
        // serait jamais suivi (donc jamais banni) une fois la map pleine — fail-open (S-11).
        self.ensure_bad_headroom(&ip, now);
        // Mise à jour de la fenêtre dans un bloc dédié : le verrou de shard `bad` (RefMut de
        // `entry`) DOIT être relâché avant toute opération sur `banned`/`bad` (sinon ré-accès au
        // même shard ⇒ interblocage). On réinitialise le compteur ici même quand on bannit.
        let should_ban = {
            let entry = self.bad.entry(ip).or_insert_with(|| {
                Mutex::new(BadWindow {
                    count: 0,
                    window_start: now,
                })
            });
            let mut w = entry.lock().unwrap_or_else(|e| e.into_inner());
            if now.saturating_duration_since(w.window_start) > self.cfg.window {
                w.count = 0;
                w.window_start = now;
            }
            w.count += 1;
            if w.count >= self.cfg.bad_requests_threshold {
                w.count = 0;
                w.window_start = now;
                true
            } else {
                false
            }
        };
        if should_ban {
            // Garantit de la place dans la map des bannissements pour qu'un nouvel abuseur
            // puisse TOUJOURS être banni (pas de fail-open quand la map est pleine, S-11).
            self.ensure_banned_headroom(now);
            self.banned.insert(ip, now + self.cfg.ban_duration);
            tracing::warn!(%ip, "IP bannie temporairement (comportement anormal)");
        }
    }

    /// Assure que `ip` pourra être suivie dans la map des fenêtres : si elle est absente et la map
    /// pleine, purge les fenêtres périmées puis, si nécessaire, évince la plus ancienne. Le coût
    /// O(n) ne survient qu'à la frontière (map pleine), jamais en régime normal (`max_tracked_ips`
    /// large ⇒ retour immédiat).
    fn ensure_bad_headroom(&self, ip: &IpAddr, now: Instant) {
        if self.bad.contains_key(ip) || self.bad.len() < self.cfg.max_tracked_ips {
            return;
        }
        let window = self.cfg.window;
        self.bad.retain(|_, w| {
            let win = w.get_mut().unwrap_or_else(|e| e.into_inner());
            now.saturating_duration_since(win.window_start) < window
        });
        if self.bad.len() < self.cfg.max_tracked_ips {
            return;
        }
        let oldest = self
            .bad
            .iter()
            .min_by_key(|e| {
                e.value()
                    .lock()
                    .map(|w| w.window_start)
                    .unwrap_or_else(|p| p.into_inner().window_start)
            })
            .map(|e| *e.key());
        if let Some(k) = oldest {
            self.bad.remove(&k);
        }
    }

    /// Assure qu'au moins une place est libre dans la map des bannissements : purge les bans
    /// expirés, puis, si elle est toujours pleine de bans actifs, évince le plus proche d'expirer.
    fn ensure_banned_headroom(&self, now: Instant) {
        if self.banned.len() < self.cfg.max_tracked_ips {
            return;
        }
        self.banned.retain(|_, until| *until > now);
        if self.banned.len() < self.cfg.max_tracked_ips {
            return;
        }
        if let Some(soonest) = self
            .banned
            .iter()
            .min_by_key(|e| *e.value())
            .map(|e| *e.key())
        {
            self.banned.remove(&soonest);
        }
    }

    pub fn prune(&self, now: Instant) {
        self.banned.retain(|_, until| *until > now);
        let window = self.cfg.window;
        self.bad.retain(|_, w| {
            let win = w.get_mut().unwrap_or_else(|e| e.into_inner());
            now.saturating_duration_since(win.window_start) < window * 2
        });
    }

    pub fn banned_count(&self) -> usize {
        self.banned.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use std::time::Duration;

    fn cfg() -> AnomalyConfig {
        AnomalyConfig {
            bad_requests_threshold: 3,
            window: Duration::from_secs(60),
            ban_duration: Duration::from_secs(300),
            max_tracked_ips: 1000,
        }
    }

    #[test]
    fn bans_after_threshold() {
        let now = Instant::now();
        let g = AnomalyGuard::new(cfg());
        let ip: IpAddr = "203.0.113.9".parse().unwrap();
        assert!(!g.is_banned(ip, now));
        g.record_bad(ip, now);
        g.record_bad(ip, now);
        assert!(!g.is_banned(ip, now));
        g.record_bad(ip, now); // 3e → ban
        assert!(g.is_banned(ip, now));
        // Le ban expire après ban_duration.
        assert!(!g.is_banned(ip, now + Duration::from_secs(301)));
    }

    #[test]
    fn window_resets_below_threshold() {
        let now = Instant::now();
        let g = AnomalyGuard::new(cfg());
        let ip: IpAddr = "203.0.113.10".parse().unwrap();
        g.record_bad(ip, now);
        g.record_bad(ip, now);
        // Plus de 60 s plus tard, le compteur repart de zéro.
        g.record_bad(ip, now + Duration::from_secs(61));
        assert!(!g.is_banned(ip, now + Duration::from_secs(61)));
    }

    #[test]
    fn forwarded_for_ignored_when_untrusted() {
        let mut h = HeaderMap::new();
        h.insert("x-forwarded-for", HeaderValue::from_static("1.2.3.4"));
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        assert_eq!(client_ip(&h, peer, false), peer);
        // Quand on fait confiance au proxy, on prend l'entrée la plus à droite.
        assert_eq!(
            client_ip(&h, peer, true),
            "1.2.3.4".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn forwarded_for_takes_rightmost() {
        let mut h = HeaderMap::new();
        h.insert(
            "x-forwarded-for",
            HeaderValue::from_static("1.1.1.1, 2.2.2.2, 3.3.3.3"),
        );
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        assert_eq!(
            client_ip(&h, peer, true),
            "3.3.3.3".parse::<IpAddr>().unwrap()
        );
    }

    #[test]
    fn forwarded_for_malformed_rightmost_does_not_fall_back_to_client_token() {
        // Le dernier hop (ajouté par le proxy) est illisible : on NE retient PAS l'entrée
        // précédente contrôlable par le client ; on retombe sur le pair (S-11).
        let mut h = HeaderMap::new();
        h.insert(
            "x-forwarded-for",
            HeaderValue::from_static("1.2.3.4, not-an-ip"),
        );
        let peer: IpAddr = "10.0.0.1".parse().unwrap();
        assert_eq!(client_ip(&h, peer, true), peer);
    }

    #[test]
    fn unspecified_ip_is_never_banned() {
        // gRPC sans remote_addr → 0.0.0.0 : ni suivi ni banni (sinon sort commun, S-9).
        let now = Instant::now();
        let g = AnomalyGuard::new(cfg());
        let unknown: IpAddr = "0.0.0.0".parse().unwrap();
        for _ in 0..10 {
            g.record_bad(unknown, now);
        }
        assert!(!g.is_banned(unknown, now));
        assert_eq!(g.banned_count(), 0);
    }

    #[test]
    fn new_abuser_banned_even_when_banned_map_full() {
        // Pas de fail-open : quand la map des bannissements est saturée de bans actifs, un nouvel
        // abuseur est tout de même banni (éviction du plus proche d'expirer) (S-11).
        let now = Instant::now();
        let cfg = AnomalyConfig {
            bad_requests_threshold: 1,
            window: Duration::from_secs(60),
            ban_duration: Duration::from_secs(300),
            max_tracked_ips: 2,
        };
        let g = AnomalyGuard::new(cfg);
        // Sature la map (2 bans actifs).
        g.record_bad("203.0.113.1".parse().unwrap(), now);
        g.record_bad("203.0.113.2".parse().unwrap(), now);
        assert_eq!(g.banned_count(), 2);
        // Un nouvel abuseur doit quand même être banni.
        let fresh: IpAddr = "203.0.113.3".parse().unwrap();
        g.record_bad(fresh, now);
        assert!(g.is_banned(fresh, now), "le nouvel abuseur doit être banni");
    }
}
