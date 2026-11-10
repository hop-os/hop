//! fakes3: een nep-S3 voor tests en om mee te spelen.
//!
//! Een bucket in het geheugen, path-style (`/<bucket>/<sleutel>`): het pad is
//! de sleutel, een bucket hoeft niet te bestaan. Elke handtekening is goed
//! (SigV4 of niets). Wat een lease nodig heeft, het CAS-protocol van S3:
//!
//! - `GET` en `HEAD`: 200 met `ETag`, of 404;
//! - `PUT`: `If-None-Match: *` maakt alleen aan als er niets is, `If-Match:
//!   <etag>` vervangt alleen die versie; anders 412. 200 met de nieuwe `ETag`;
//! - `DELETE`: met `If-Match` alleen die versie (anders 412); 204, ook als er
//!   niets was;
//! - `GET /health`: 200.
//!
//! Een `ETag` is `"<n>"` met een teller over de hele store; `If-Match` mag hem
//! met of zonder aanhalingstekens geven. Bij de start één regel `FAKES3_UP
//! addr=<adres>` op stdout. Alles is weg als het proces stopt.
//!
//! ```sh
//! cargo run --release -p fakes3 -- -listen 127.0.0.1:9000
//! ```
//!
//! Hop als cluster erop (`hopos.lock.type=s3`, `hopos.s3.endpoint=http://…:9000`,
//! `hopos.s3.pathstyle=1`, en een willekeurige bucket, sleutel en secret):
//! `tools/qemu-test-cluster.sh` doet het zo.
//!
//! Elke verbinding heeft een eigen thread; de objecten heeft er één, de
//! eigenaar, en de verbindingen vragen hem via een kanaal. Geen mutex.

#![cfg_attr(test, allow(clippy::unwrap_used))]

use hostnet::{StdConn, block_on};
use leanhttp::Exchange;
use std::collections::BTreeMap;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{Sender, channel};
use std::thread;
use std::time::Duration;

/// Eén verzoek aan de eigenaar van de objecten.
struct Ask {
    method: String,
    key: String,
    if_match: String,
    if_none_match: String,
    body: Vec<u8>,
    reply: Sender<Answer>,
}

/// Het antwoord van de eigenaar.
struct Answer {
    status: u16,
    etag: Option<String>,
    body: Vec<u8>,
}

impl Answer {
    fn status(status: u16) -> Self {
        Self {
            status,
            etag: None,
            body: Vec::new(),
        }
    }
}

/// De objecten: sleutel naar (ETag, inhoud).
#[derive(Default)]
struct Objects {
    map: BTreeMap<String, (String, Vec<u8>)>,
    next: u64,
}

/// Dezelfde ETag, met of zonder aanhalingstekens.
fn same(stored: &str, asked: &str) -> bool {
    stored.trim_matches('"') == asked.trim_matches('"')
}

impl Objects {
    fn handle(&mut self, a: Ask) -> Answer {
        let current = self.map.get(&a.key);
        match a.method.as_str() {
            "GET" | "HEAD" => match current {
                Some((etag, body)) => Answer {
                    status: 200,
                    etag: Some(etag.clone()),
                    body: if a.method == "GET" {
                        body.clone()
                    } else {
                        Vec::new()
                    },
                },
                None => Answer::status(404),
            },
            "PUT" => {
                if a.if_none_match == "*" && current.is_some() {
                    return Answer::status(412);
                }
                if !a.if_match.is_empty() && !current.is_some_and(|(e, _)| same(e, &a.if_match)) {
                    return Answer::status(412);
                }
                self.next += 1;
                let etag = format!("\"{}\"", self.next);
                self.map.insert(a.key, (etag.clone(), a.body));
                Answer {
                    status: 200,
                    etag: Some(etag),
                    body: Vec::new(),
                }
            }
            "DELETE" => {
                if let Some((e, _)) = current
                    && !a.if_match.is_empty()
                    && !same(e, &a.if_match)
                {
                    return Answer::status(412);
                }
                self.map.remove(&a.key);
                Answer::status(204)
            }
            _ => Answer::status(405),
        }
    }
}

/// Bedient één verbinding, verzoek na verzoek, tot de client sluit.
fn serve(stream: TcpStream, owner: &Sender<Ask>) {
    let conn = StdConn::new(stream, Some(Duration::from_secs(60)));
    let _ = block_on(hostnet::serve_conn(
        conn,
        async |ex: &mut Exchange<'_, StdConn<TcpStream>>| {
            let body = ex.read_body_to_end().await?;
            let path = ex.req.path.split('?').next().unwrap_or("").to_string();
            if path == "/health" {
                ex.write_header(200)?;
                ex.write(b"ok\n").await?;
                return Ok(());
            }
            let header = |name: &str| {
                ex.req
                    .header
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map_or_else(String::new, |(_, v)| v.to_string())
            };
            let (tx, rx) = channel();
            let ask = Ask {
                method: ex.req.method.clone(),
                key: path,
                if_match: header("If-Match"),
                if_none_match: header("If-None-Match"),
                body,
                reply: tx,
            };
            let a = match owner.send(ask).ok().and_then(|()| rx.recv().ok()) {
                Some(a) => a,
                None => Answer::status(500),
            };
            if let Some(e) = &a.etag {
                ex.header_mut().set("ETag", e)?;
            }
            ex.write_header(a.status)?;
            if !a.body.is_empty() {
                ex.write(&a.body).await?;
            }
            Ok(())
        },
    ));
}

fn main() {
    let mut listen = String::from("127.0.0.1:9000");
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "-listen" | "--listen" => listen = args.next().unwrap_or_default(),
            _ => {
                eprintln!("usage: fakes3 [-listen host:port]");
                std::process::exit(2);
            }
        }
    }
    let listener = match TcpListener::bind(&listen) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("fakes3: listen {listen}: {e}");
            std::process::exit(1);
        }
    };
    let addr = listener.local_addr().map_or(listen, |a| a.to_string());
    let (owner, asks) = channel::<Ask>();
    thread::spawn(move || {
        let mut objects = Objects::default();
        for a in asks {
            let reply = a.reply.clone();
            let _ = reply.send(objects.handle(a));
        }
    });
    println!("FAKES3_UP addr={addr}");
    for stream in listener.incoming().flatten() {
        let owner = owner.clone();
        thread::spawn(move || serve(stream, &owner));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ask(method: &str, key: &str, if_match: &str, if_none: &str, body: &[u8]) -> Ask {
        Ask {
            method: method.into(),
            key: key.into(),
            if_match: if_match.into(),
            if_none_match: if_none.into(),
            body: body.to_vec(),
            reply: channel().0,
        }
    }

    #[test]
    fn the_cas_of_a_lease() {
        let mut o = Objects::default();
        assert_eq!(o.handle(ask("GET", "/b/lease", "", "", b"")).status, 404);
        let made = o.handle(ask("PUT", "/b/lease", "", "*", b"een"));
        assert_eq!(made.status, 200);
        let e1 = made.etag.unwrap();
        // Nog eens aanmaken: iemand heeft hem al.
        assert_eq!(
            o.handle(ask("PUT", "/b/lease", "", "*", b"twee")).status,
            412
        );
        // Vervangen met de oude ETag, kaal of geciteerd.
        assert_eq!(
            o.handle(ask("PUT", "/b/lease", "\"999\"", "", b"x")).status,
            412
        );
        let e2 = o
            .handle(ask("PUT", "/b/lease", e1.trim_matches('"'), "", b"twee"))
            .etag
            .unwrap();
        assert_ne!(e1, e2);
        let got = o.handle(ask("GET", "/b/lease", "", "", b""));
        assert_eq!((got.status, got.body), (200, b"twee".to_vec()));
        assert!(
            o.handle(ask("HEAD", "/b/lease", "", "", b""))
                .body
                .is_empty()
        );
        // Wissen met een verkeerde ETag mag niet, met de goede wel.
        assert_eq!(
            o.handle(ask("DELETE", "/b/lease", &e1, "", b"")).status,
            412
        );
        assert_eq!(
            o.handle(ask("DELETE", "/b/lease", &e2, "", b"")).status,
            204
        );
        assert_eq!(o.handle(ask("GET", "/b/lease", "", "", b"")).status, 404);
        assert_eq!(o.handle(ask("DELETE", "/b/lease", "", "", b"")).status, 204);
    }
}
