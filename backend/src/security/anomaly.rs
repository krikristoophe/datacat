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
        if let Some(ip) = xff
            .split(',')
            .rev()
            .filter_map(|s| s.trim().parse::<IpAddr>().ok())
            .next()
        {
            return ip;
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
        if self.banned.len() < self.cfg.max_tracked_ips || self.banned.contains_key(&ip) {
            let entry = self.bad.entry(ip).or_insert_with(|| {
                Mutex::new(BadWindow {
                    count: 0,
                    window_start: now,
                })
            });
            let mut w = entry.lock().expect("verrou anomalie empoisonné");
            if now.saturating_duration_since(w.window_start) > self.cfg.window {
                w.count = 0;
                w.window_start = now;
            }
            w.count += 1;
            if w.count >= self.cfg.bad_requests_threshold {
                let until = now + self.cfg.ban_duration;
                self.banned.insert(ip, until);
                w.count = 0;
                w.window_start = now;
                tracing::warn!(%ip, "IP bannie temporairement (comportement anormal)");
            }
        }
    }

    pub fn prune(&self, now: Instant) {
        self.banned.retain(|_, until| *until > now);
        let window = self.cfg.window;
        self.bad.retain(|_, w| {
            let win = w.get_mut().expect("verrou anomalie empoisonné");
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
}
