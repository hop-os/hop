//! De routetabellen van de twee API's: een `leanhttp::Mux` over een
//! `Copy`-enum per API, en de opzoeking voor een [`Request`] van deze crate.
//!
//! De Mux kent alleen `leanhttp::Request`; de handlers hier werken op het
//! eigen [`Request`]. Voor de opzoeking gaan methode en pad mee naar een
//! `leanhttp::Request`, en de wildcardwaarden komen daaruit terug.
//!
//! Een methode buiten [`Method`](crate::Method) (HEAD, PUT, ...) heet hier `OTHER`, zoals
//! [`Method::as_str`](crate::Method::as_str) hem noemt: die past op geen enkele route met een
//! methode. Een `GET`-route bedient hier dus geen HEAD (de Mux zelf laat
//! GET ook voor HEAD gelden), zoals de routering met de hand van vóór
//! 06-11-2026.
//!
//! De API's sturen nooit een 405 uit de Mux: een pad dat bestaat voor een
//! andere methode, is hier [`Lookup::Miss`], en de API antwoordt zoals
//! vroeger op een onbekende route (404 `not found`).

use alloc::string::String;
use core::fmt;

use leanhttp::{Found, Mux};

use crate::Request;

/// Een routetabel: de Mux, of `None` als een patroon niet registreerde.
pub(crate) struct Routes<T>(Option<Mux<T>>);

/// Wat [`Routes::find`] bij een verzoek vond.
pub(crate) enum Lookup<T> {
    /// De route, met het verzoek waar de Mux de wildcardwaarden op zette.
    Hit(T, leanhttp::Request),
    /// Geen route voor deze methode en dit pad.
    Miss,
    /// De tabel registreerde niet, of de opzoeking had geen geheugen.
    Broken,
}

impl<T: Copy> Routes<T> {
    /// Registreert `table` in volgorde. Een patroon dat niet registreert, is
    /// een bedradingsfout (de tests bouwen elke tabel); dan blijft de tabel
    /// leeg en antwoordt elke opzoeking [`Lookup::Broken`].
    pub(crate) fn new(table: &[(&str, T)]) -> Self {
        let mut mux = Mux::new();
        for (pattern, value) in table {
            if mux.handle(pattern, *value).is_err() {
                return Self(None);
            }
        }
        Self(Some(mux))
    }

    /// Of elk patroon registreerde.
    #[cfg(test)]
    pub(crate) fn is_built(&self) -> bool {
        self.0.is_some()
    }

    /// De route voor methode en pad van `req`.
    pub(crate) fn find(&self, req: &Request) -> Lookup<T> {
        let Some(mux) = &self.0 else {
            return Lookup::Broken;
        };
        let mut lr = leanhttp::Request::default();
        lr.method = String::from(req.method.as_str());
        lr.path = req.path.clone();
        match mux.find(&mut lr) {
            Ok(Found::Route(v)) => Lookup::Hit(*v, lr),
            Ok(Found::NotFound | Found::MethodNotAllowed(_)) => Lookup::Miss,
            Err(_) => Lookup::Broken,
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Routes<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Routes").field(&self.0).finish()
    }
}

/// De waarde van wildcard `name`, of leeg.
pub(crate) fn value<'r>(lr: &'r leanhttp::Request, name: &str) -> &'r str {
    lr.path_value(name).unwrap_or("")
}
