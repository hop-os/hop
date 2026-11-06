//! De admin-poort: `/health` en `/metrics` (Go: de `adminMux` in
//! `cmd/hoplb/main.go`).
//!
//! Een eigen poort, zodat een firewall hem kan dichthouden terwijl het
//! verkeer open staat (README, "Why separate ports?"). Bezit niets: de tekst
//! van `/metrics` komt van wie de [`crate::Metrics`] bezit, via de
//! `scrape`-functie van de schil.

use alloc::string::String;

use leanhttp::{Conn, Exchange, Found, Mux};

use crate::metrics::CONTENT_TYPE;

/// De body van `/health` (Go: `fmt.Fprintln(w, "ok")`).
pub const HEALTH_BODY: &[u8] = b"ok\n";

/// De routes van de admin-poort.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Page {
    /// `/health`: 200, `ok`.
    Health,
    /// `/metrics`: de Prometheus-tekst.
    Metrics,
}

/// De routetabel. Go registreerde beide zonder methode, dus elke methode
/// telt: geen route draagt er een, en een 405 komt er nooit uit.
///
/// # Errors
///
/// Een bedradingsfout of een volle heap, vóór het serveren.
pub fn mux() -> leanhttp::Result<Mux<Page>> {
    let mut mux = Mux::default();
    mux.handle("/health", Page::Health)?;
    mux.handle("/metrics", Page::Metrics)?;
    Ok(mux)
}

/// Beantwoordt één verzoek op de admin-poort. De verbinding is van de
/// schil: een thread per verbinding op de host (`hostnet::serve_conn`),
/// `applibhttp` op HopOS.
///
/// `scrape` geeft de tekst van `/metrics`, of `None` als de eigenaar niet
/// antwoordt (dan 503). De rest is 404, zoals Go's `ServeMux` (`404 page
/// not found`).
///
/// # Errors
///
/// De fout van leanhttp; de verbinding gaat dan dicht.
pub async fn handle<C, S>(ex: &mut Exchange<'_, C>, mux: &Mux<Page>, scrape: S) -> leanhttp::Result
where
    C: Conn,
    S: AsyncFnOnce() -> Option<String>,
{
    match mux.find(&mut ex.req)? {
        Found::Route(Page::Health) => {
            ex.header_mut()
                .set("Content-Type", "text/plain; charset=utf-8")?;
            ex.write(HEALTH_BODY).await?;
            Ok(())
        }
        Found::Route(Page::Metrics) => match scrape().await {
            Some(text) => {
                ex.header_mut().set("Content-Type", CONTENT_TYPE)?;
                ex.write(text.as_bytes()).await?;
                Ok(())
            }
            None => ex.error(503, "metrics unavailable").await,
        },
        // Geen route draagt een methode, dus MethodNotAllowed kan niet; mocht
        // het toch, dan is het dezelfde 404 als een onbekend pad.
        Found::NotFound | Found::MethodNotAllowed(_) => {
            ex.header_mut().set("X-Content-Type-Options", "nosniff")?;
            ex.error(404, "404 page not found").await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{Pipe, block_on};

    fn ask(method: &str, path: &str) -> String {
        let req =
            alloc::format!("{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
        let pipe = Pipe::new(&[req.as_bytes()]);
        let mux = mux().unwrap();
        let out = block_on(leanhttp::serve(
            pipe.clone(),
            async |ex: &mut Exchange<'_, Pipe>| {
                handle(ex, &mux, async || Some(String::from("# metrics\n"))).await
            },
        ));
        assert!(matches!(out, Ok(leanhttp::Outcome::Closed)));
        pipe.text()
    }

    fn get(path: &str) -> String {
        ask("GET", path)
    }

    #[test]
    fn health_metrics_and_the_rest() {
        let h = get("/health");
        assert!(h.starts_with("HTTP/1.1 200 "), "{h}");
        assert!(h.ends_with("\r\n\r\nok\n"), "{h}");
        let m = get("/metrics");
        assert!(
            m.contains("Content-Type: text/plain; version=0.0.4\r\n"),
            "{m}"
        );
        assert!(m.ends_with("# metrics\n"), "{m}");
        let n = get("/nope");
        assert!(n.starts_with("HTTP/1.1 404 "), "{n}");
        assert!(n.ends_with("404 page not found\n"), "{n}");
    }

    #[test]
    fn every_method_counts_and_a_trailing_slash_is_another_path() {
        // Go registreerde zonder methode: een POST krijgt de pagina, geen 405.
        let p = ask("POST", "/health");
        assert!(p.starts_with("HTTP/1.1 200 "), "{p}");
        let d = ask("DELETE", "/metrics");
        assert!(d.ends_with("# metrics\n"), "{d}");
        let s = get("/health/");
        assert!(s.starts_with("HTTP/1.1 404 "), "{s}");
        assert!(s.contains("X-Content-Type-Options: nosniff\r\n"), "{s}");
    }
}
