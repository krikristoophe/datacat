//! Rate limiting à deux niveaux + filet global (cf. cahier §7.2).
//!
//! - **Filet global** : token bucket unique, protège l'infrastructure d'un flood massif.
//! - **Par session** : token bucket par `session_id` — empêche une session d'impacter ses
//!   collègues (indispensable en B2B où un établissement sort derrière une IP NAT unique).
//! - **Plafond de sessions par IP** : fenêtre glissante comptant les sessions distinctes par
//!   IP — referme le contournement « générer des milliers de fausses sessions ».
//!
//! `Instant` est injecté dans `check` pour des tests déterministes.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::config::RateLimitConfig;

#[derive(Debug, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny {
        scope: &'static str,
        retry_after_secs: u64,
    },
}

/// Token bucket classique (capacité = burst, recharge = `rate`/seconde).
struct Bucket {
    tokens: f64,
    last: Instant,
    rate: f64,
    burst: f64,
}

impl Bucket {
    fn new(rate: f64, burst: f64, now: Instant) -> Self {
        Self {
            tokens: burst,
            last: now,
            rate,
            burst,
        }
    }

    /// Tente de consommer `n` jetons. En cas d'échec, retourne le délai d'attente (s).
    fn try_take(&mut self, n: f64, now: Instant) -> Result<(), f64> {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        self.last = now;
        if self.tokens >= n {
            self.tokens -= n;
            Ok(())
        } else {
            let deficit = n - self.tokens;
            Err(if self.rate > 0.0 {
                deficit / self.rate
            } else {
                f64::from(u32::MAX)
            })
        }
    }
}

struct SessionState {
    bucket: Bucket,
    last_seen: Instant,
}

/// Fenêtre glissante des sessions distinctes vues depuis une IP.
struct IpWindow {
    sessions: HashMap<String, Instant>,
    last_seen: Instant,
}

impl IpWindow {
    /// Enregistre `session` ; retourne `Err(retry_after)` si le plafond est dépassé.
    fn record(
        &mut self,
        session: &str,
        now: Instant,
        cap: u64,
        window: Duration,
    ) -> Result<(), f64> {
        self.last_seen = now;
        // Purge des sessions sorties de la fenêtre.
        self.sessions
            .retain(|_, seen| now.saturating_duration_since(*seen) < window);

        if let Some(seen) = self.sessions.get_mut(session) {
            *seen = now; // session déjà connue : on rafraîchit, pas de nouvelle entrée
            return Ok(());
        }
        if self.sessions.len() as u64 >= cap {
            // Délai jusqu'à libération de la plus ancienne session.
            let oldest = self
                .sessions
                .values()
                .map(|seen| now.saturating_duration_since(*seen))
                .max()
                .unwrap_or(window);
            let wait = window.saturating_sub(oldest).as_secs_f64();
            return Err(wait.max(1.0));
        }
        self.sessions.insert(session.to_string(), now);
        Ok(())
    }
}

pub struct RateLimiter {
    cfg: RateLimitConfig,
    global: Mutex<Bucket>,
    sessions: DashMap<String, Mutex<SessionState>>,
    ips: DashMap<IpAddr, Mutex<IpWindow>>,
}

impl RateLimiter {
    pub fn new(cfg: RateLimitConfig, now: Instant) -> Self {
        let global = Mutex::new(Bucket::new(cfg.global_per_sec, cfg.global_burst, now));
        Self {
            cfg,
            global,
            sessions: DashMap::new(),
            ips: DashMap::new(),
        }
    }

    /// Vérifie les trois niveaux pour une requête de `n` events depuis `ip`/`session_id`.
    ///
    /// Ordre : session → IP → filet global. Le global est débité **en dernier** pour qu'une rafale
    /// refusée plus finement (session/IP) ne puisse pas vider à bas coût le bucket global (S-10).
    /// Les verrous sont tolérants à l'empoisonnement (jamais de panic en chemin de requête, S-1).
    pub fn check(&self, now: Instant, ip: IpAddr, session_id: &str, n: u32) -> Decision {
        let n = f64::from(n.max(1));

        // 1. Limite fine par session. `entry().or_insert_with` crée le bucket de façon atomique
        // (l'ancien get-puis-insert pouvait écraser/réinitialiser un bucket sous concurrence, S-10).
        if !self.sessions.contains_key(session_id)
            && self.sessions.len() >= self.cfg.max_tracked_sessions
        {
            return deny("session", 1.0); // borne mémoire : on refuse les sessions inédites
        }
        {
            let entry = self
                .sessions
                .entry(session_id.to_string())
                .or_insert_with(|| {
                    Mutex::new(SessionState {
                        bucket: Bucket::new(self.cfg.session_per_sec, self.cfg.session_burst, now),
                        last_seen: now,
                    })
                });
            let mut s = entry.lock().unwrap_or_else(|e| e.into_inner());
            s.last_seen = now;
            if let Err(wait) = s.bucket.try_take(n, now) {
                return deny("session", wait);
            }
        }

        // 2. Plafond de sessions distinctes par IP. Ignoré quand l'IP du pair est inconnue
        // (UNSPECIFIED, p.ex. gRPC sans `remote_addr`) : sinon tous ces clients s'effondrent sur
        // 0.0.0.0 et se partagent un plafond / un bannissement (sort commun, S-9).
        if !ip.is_unspecified() {
            if !self.ips.contains_key(&ip) && self.ips.len() >= self.cfg.max_tracked_ips {
                return deny("ip_sessions", 1.0);
            }
            let entry = self.ips.entry(ip).or_insert_with(|| {
                Mutex::new(IpWindow {
                    sessions: HashMap::new(),
                    last_seen: now,
                })
            });
            let mut win = entry.lock().unwrap_or_else(|e| e.into_inner());
            if let Err(wait) = win.record(
                session_id,
                now,
                self.cfg.ip_session_cap,
                self.cfg.ip_session_window,
            ) {
                return deny("ip_sessions", wait);
            }
        }

        // 3. Filet global, débité en dernier (cf. supra).
        if let Err(wait) = self
            .global
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .try_take(n, now)
        {
            return deny("global", wait);
        }

        Decision::Allow
    }

    /// Purge périodique des états inactifs (évite la croissance mémoire non bornée).
    pub fn prune(&self, now: Instant) {
        let session_ttl = self.cfg.ip_session_window.max(Duration::from_secs(300)) * 2;
        self.sessions.retain(|_, state| {
            // Référence exclusive ⇒ pas besoin de verrouiller (tolérant à l'empoisonnement).
            let s = state.get_mut().unwrap_or_else(|e| e.into_inner());
            now.saturating_duration_since(s.last_seen) < session_ttl
        });
        let window = self.cfg.ip_session_window;
        self.ips.retain(|_, w| {
            let win = w.get_mut().unwrap_or_else(|e| e.into_inner());
            win.sessions
                .retain(|_, seen| now.saturating_duration_since(*seen) < window);
            !win.sessions.is_empty()
        });
    }

    pub fn tracked_sessions(&self) -> usize {
        self.sessions.len()
    }
    pub fn tracked_ips(&self) -> usize {
        self.ips.len()
    }
}

fn deny(scope: &'static str, wait_secs: f64) -> Decision {
    Decision::Deny {
        scope,
        retry_after_secs: wait_secs.ceil().max(1.0) as u64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> RateLimitConfig {
        RateLimitConfig {
            global_per_sec: 1_000_000.0,
            global_burst: 1_000_000.0,
            session_per_sec: 10.0,
            session_burst: 20.0,
            ip_session_cap: 3,
            ip_session_window: Duration::from_secs(60),
            max_tracked_sessions: 100_000,
            max_tracked_ips: 100_000,
        }
    }

    fn ip() -> IpAddr {
        "203.0.113.7".parse().unwrap()
    }

    #[test]
    fn session_limit_isolated_per_session() {
        let t0 = Instant::now();
        let rl = RateLimiter::new(cfg(), t0);
        // La session "abuser" épuise son bucket (burst 20).
        let d = rl.check(t0, ip(), "abuser", 20);
        assert_eq!(d, Decision::Allow);
        let d = rl.check(t0, ip(), "abuser", 1);
        assert!(matches!(
            d,
            Decision::Deny {
                scope: "session",
                ..
            }
        ));
        // Un collègue derrière la MÊME IP n'est pas impacté.
        let d = rl.check(t0, ip(), "colleague", 5);
        assert_eq!(d, Decision::Allow);
    }

    #[test]
    fn session_bucket_refills_over_time() {
        let t0 = Instant::now();
        let rl = RateLimiter::new(cfg(), t0);
        assert_eq!(rl.check(t0, ip(), "s", 20), Decision::Allow);
        assert!(matches!(
            rl.check(t0, ip(), "s", 1),
            Decision::Deny {
                scope: "session",
                ..
            }
        ));
        // +2 s → 20 jetons rechargés (10/s).
        let t1 = t0 + Duration::from_secs(2);
        assert_eq!(rl.check(t1, ip(), "s", 10), Decision::Allow);
    }

    #[test]
    fn ip_session_cap_blocks_fake_session_flood() {
        let t0 = Instant::now();
        let rl = RateLimiter::new(cfg(), t0);
        // 3 sessions distinctes autorisées (cap = 3).
        assert_eq!(rl.check(t0, ip(), "s1", 1), Decision::Allow);
        assert_eq!(rl.check(t0, ip(), "s2", 1), Decision::Allow);
        assert_eq!(rl.check(t0, ip(), "s3", 1), Decision::Allow);
        // La 4e session distincte depuis la même IP est refusée.
        assert!(matches!(
            rl.check(t0, ip(), "s4", 1),
            Decision::Deny {
                scope: "ip_sessions",
                ..
            }
        ));
        // Une session déjà connue passe toujours (pas une nouvelle session).
        assert_eq!(rl.check(t0, ip(), "s1", 1), Decision::Allow);
    }

    #[test]
    fn ip_session_window_slides() {
        let t0 = Instant::now();
        let rl = RateLimiter::new(cfg(), t0);
        for s in ["s1", "s2", "s3"] {
            assert_eq!(rl.check(t0, ip(), s, 1), Decision::Allow);
        }
        assert!(matches!(
            rl.check(t0, ip(), "s4", 1),
            Decision::Deny {
                scope: "ip_sessions",
                ..
            }
        ));
        // Après la fenêtre (61 s), les anciennes sessions sont sorties → s4 passe.
        let t1 = t0 + Duration::from_secs(61);
        assert_eq!(rl.check(t1, ip(), "s4", 1), Decision::Allow);
    }

    #[test]
    fn unspecified_ip_skips_per_ip_cap() {
        // gRPC sans remote_addr → IP UNSPECIFIED : pas de plafond par IP (sinon tous les clients
        // partagent 0.0.0.0). Le filet par session reste actif (S-9).
        let t0 = Instant::now();
        let rl = RateLimiter::new(cfg(), t0);
        let unknown: IpAddr = "0.0.0.0".parse().unwrap();
        // Bien au-delà du cap de 3 sessions distinctes : tout passe car l'IP est ignorée.
        for i in 0..10 {
            let s = format!("s{i}");
            assert_eq!(rl.check(t0, unknown, &s, 1), Decision::Allow);
        }
        assert_eq!(
            rl.tracked_ips(),
            0,
            "aucune IP inconnue ne doit être suivie"
        );
    }

    #[test]
    fn different_ips_independent() {
        let t0 = Instant::now();
        let rl = RateLimiter::new(cfg(), t0);
        let ip_a: IpAddr = "203.0.113.1".parse().unwrap();
        let ip_b: IpAddr = "203.0.113.2".parse().unwrap();
        for s in ["s1", "s2", "s3"] {
            assert_eq!(rl.check(t0, ip_a, s, 1), Decision::Allow);
        }
        assert!(matches!(
            rl.check(t0, ip_a, "s4", 1),
            Decision::Deny {
                scope: "ip_sessions",
                ..
            }
        ));
        // IP B repart de zéro.
        assert_eq!(rl.check(t0, ip_b, "s4", 1), Decision::Allow);
    }
}
