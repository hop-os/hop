//! De agent-API van een node (poort P): taken aannemen, stoppen, tonen.
//!
//! Bezit de routes en hun antwoorden over een geleende [`agent::Agent`]; de
//! staat blijft van de agent. De clusterroutes (`/v1/...`) gaan als
//! [`Effect::Proxy`] naar de leader die de lus bevestigde.

use alloc::string::String;
use alloc::vec::Vec;

use agent::{Agent, Error as AgentError};
use types::json::{self, Value};
use types::{Job, Nanos};

use crate::routes::{Lookup, Routes, value};
use crate::{Method, Request, Response, check_auth, reply, s};

/// Hoe groot een body is die de proxy naar de leader doorgeeft.
///
/// De proxy moet de body lezen voordat hij hem doorgeeft, en een onbegrensde
/// lees op een route die nog niet geauthenticeerd is, is een geheugen-DoS
/// vóór de toets. Zelfde maat als in Go (`proxyMaxBody`, 8 MiB).
pub const PROXY_MAX_BODY: usize = 8 << 20;

/// Welke logstroom.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogStream {
    /// Standaard uitvoer.
    Stdout,
    /// Standaard fout.
    Stderr,
}

/// Wat de adapter na het antwoord nog moet doen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Niets: het antwoord is compleet.
    None,
    /// Geef het verzoek door aan de leader en stuur zijn antwoord terug.
    ///
    /// Met `stream` per brok doorspoelen (SSE, log-tails); anders gebufferd
    /// met een timeout. De `X-Hop-Auth` van de aanroeper gaat mee: de proxy
    /// geeft dezelfde methode, pad en body door, dus de handtekening blijft
    /// geldig (het hele cluster deelt één sleutel).
    Proxy {
        /// Het leader-adres (`ip:poort`).
        leader: String,
        /// Of het antwoord een stroom is.
        stream: bool,
    },
    /// Stroom de logs van een taak als SSE (de adapter vraagt de runner; 404 als die hem niet kent).
    Logs {
        /// Het taak-id.
        task_id: String,
        /// De stroom.
        stream: LogStream,
    },
    /// Vervang de kern van de node (HopOS-flip): haal, toets, spring.
    Flip {
        /// De absolute URL van de bundel.
        url: String,
        /// De sha256 in hex.
        sha256: String,
    },
    /// Herstart Hop zelf (HopOS): zijn staat bij de kern, de kern stopt hem
    /// en zet een nieuwe in zijn slot; de apps draaien door.
    Restart {
        /// De absolute URL van een nieuw image van Hop; leeg is hetzelfde image.
        url: String,
        /// De sha256 van dat image in hex; leeg zonder `url`.
        sha256: String,
    },
}

/// Een route van de agent-API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Route {
    /// `/health`, vóór de HMAC.
    Health,
    /// `/leader`, vóór de HMAC.
    Leader,
    /// De clusterroutes onder `/v1`: door naar de leader.
    Proxy,
    /// `/logs/{taak}/{stroom}`.
    Logs,
    /// `/capacity`.
    Capacity,
    /// `/tasks`.
    Tasks,
    /// `/run`.
    Run,
    /// `/flip`.
    Flip,
    /// `/restart`.
    Restart,
    /// `/delete/{job}`.
    Delete,
    /// `/stop/{job}`.
    Stop,
    /// `/stop-task/{taak}`.
    StopTask,
}

/// De routetabel van de agent-API.
///
/// Zonder methode: elke route kiest zelf wat een verkeerde methode krijgt
/// (een 405 bij `/run`, `/flip`, `/restart` en de drie onder `/delete/`,
/// `/stop/` en `/stop-task/`; de rest bedient elke methode, zoals Go).
/// Een naam of id met een `/` erin blijft een naam (`{...}` vangt de rest),
/// en een lege (`/delete/`) een 400 in de handler.
const ROUTES: [(&str, Route); 18] = [
    ("/health", Route::Health),
    ("/leader", Route::Leader),
    ("/v1/agents", Route::Proxy),
    ("/v1/agents/", Route::Proxy),
    ("/v1/jobs", Route::Proxy),
    ("/v1/jobs/", Route::Proxy),
    ("/v1/status", Route::Proxy),
    ("/v1/tasks", Route::Proxy),
    ("/v1/events", Route::Proxy),
    ("/logs/{rest...}", Route::Logs),
    ("/capacity", Route::Capacity),
    ("/tasks", Route::Tasks),
    ("/run", Route::Run),
    ("/flip", Route::Flip),
    ("/restart", Route::Restart),
    ("/delete/{name...}", Route::Delete),
    ("/stop/{name...}", Route::Stop),
    ("/stop-task/{id...}", Route::StopTask),
];

/// De agent-API.
#[derive(Debug)]
pub struct NodeApi {
    key: Vec<u8>,
    /// De node is een HopOS-node: `/flip` en `/restart` gaan naar de kern.
    flip_enabled: bool,
    routes: Routes<Route>,
}

impl Default for NodeApi {
    fn default() -> Self {
        Self::new(b"", false)
    }
}

impl Clone for NodeApi {
    fn clone(&self) -> Self {
        Self::new(&self.key, self.flip_enabled)
    }
}

impl NodeApi {
    /// Een API met HMAC-sleutel `key` (leeg = geen authenticatie);
    /// `flip_enabled` zet de routes van de kern aan (`/flip`, `/restart`).
    pub fn new(key: &[u8], flip_enabled: bool) -> Self {
        Self {
            key: key.to_vec(),
            flip_enabled,
            routes: Routes::new(&ROUTES),
        }
    }

    /// Behandelt één verzoek.
    ///
    /// `pool_largest` is de grootste partitie die de HopOS-runner nog kan
    /// plaatsen (voor de toelating van `/run`).
    pub fn handle(
        &self,
        agent: &mut Agent,
        now: Nanos,
        pool_largest: Option<u64>,
        req: &Request,
    ) -> (Response, Effect) {
        let (mut resp, effect) = self.route(agent, now, pool_largest, req);
        cors(req, &mut resp);
        (resp, effect)
    }

    fn route(
        &self,
        agent: &mut Agent,
        now: Nanos,
        pool_largest: Option<u64>,
        req: &Request,
    ) -> (Response, Effect) {
        let none = |r| (r, Effect::None);
        if req.method == Method::Options {
            return none(Response::empty(200));
        }
        let (route, lr) = match self.routes.find(req) {
            Lookup::Hit(route, lr) => (Some(route), lr),
            Lookup::Miss => (None, leanhttp::Request::default()),
            Lookup::Broken => return none(Response::error(500, "internal error")),
        };
        // `/health` en `/leader` vóór de HMAC; een onbekend pad erna.
        if !matches!(route, Some(Route::Health | Route::Leader))
            && let Some(reject) = check_auth(&self.key, req)
        {
            return none(reject);
        }
        let Some(route) = route else {
            return none(Response::error(404, "not found"));
        };
        let r = match route {
            Route::Health => reply(200, [("status", s("ok"))]),
            Route::Leader => leader(agent),
            Route::Proxy => return proxy(agent, req),
            Route::Logs => return logs(value(&lr, "rest")),
            Route::Capacity => capacity(agent),
            Route::Tasks => tasks(agent),
            Route::Run => run(agent, now, pool_largest, req),
            Route::Flip => return self.flip(req),
            Route::Restart => return self.restart(req),
            Route::Delete => delete(agent, now, req, value(&lr, "name")),
            Route::Stop => stop(agent, req, value(&lr, "name")),
            Route::StopTask => stop_task(agent, req, value(&lr, "id")),
        };
        none(r)
    }

    /// `/flip`: de node vervangt zijn eigen kern terwijl de taken doordraaien.
    ///
    /// Achter dezelfde HMAC als dispatch, want wie een kern mag aanleveren mag
    /// alles. Het antwoord is een 202 en niet de uitkomst: een geslaagde flip
    /// keert per definitie nooit terug.
    ///
    /// Alleen warm (Derek, 01-11): een body met `cold` is een 400, geen
    /// stille warme flip.
    fn flip(&self, req: &Request) -> (Response, Effect) {
        let none = |r| (r, Effect::None);
        if req.method != Method::Post {
            return none(Response::error(405, "method not allowed"));
        }
        if !self.flip_enabled {
            return none(Response::error(
                501,
                "this node cannot flip its kernel (not a HopOS node, or flipping is disabled)",
            ));
        }
        let Ok(v) = json::parse(&req.body) else {
            return none(Response::error(400, "invalid json"));
        };
        let url = v
            .as_object()
            .and_then(|o| o.get("url"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let sum = v
            .as_object()
            .and_then(|o| o.get("sha256"))
            .and_then(Value::as_str)
            .unwrap_or("");
        if !url.contains("://") {
            return none(Response::error(400, "url must be absolute"));
        }
        if sum.len() != 64 || !sum.bytes().all(|c| c.is_ascii_hexdigit()) {
            return none(Response::error(400, "sha256 must be 64 hex characters"));
        }
        if v.as_object().is_some_and(|o| o.get("cold").is_some()) {
            return none(Response::error(
                400,
                "there is no cold flip; a flip is warm",
            ));
        }
        let effect = Effect::Flip {
            url: String::from(url),
            sha256: String::from(sum),
        };
        let msg = "flip requested: the node fetches, verifies and replaces its kernel; watch its console and its re-registration";
        (reply(202, [("status", s(msg))]), effect)
    }

    /// `/restart`: Hop herstart zichzelf terwijl de apps doordraaien.
    ///
    /// Achter dezelfde HMAC als `/flip`: wie een image van Hop mag
    /// aanleveren, mag alles. Zonder body (of `{}`) hetzelfde image; met
    /// `{"url": ..., "sha256": ...}` een nieuw, dat de node eerst ophaalt.
    /// Het antwoord is een 202 en niet de uitkomst: wie antwoordt, is de
    /// Hop die zo stopt; de volgende neemt zijn kooien over.
    fn restart(&self, req: &Request) -> (Response, Effect) {
        let none = |r| (r, Effect::None);
        if req.method != Method::Post {
            return none(Response::error(405, "method not allowed"));
        }
        if !self.flip_enabled {
            return none(Response::error(
                501,
                "this node cannot restart Hop under its apps (not a HopOS node)",
            ));
        }
        let parsed;
        let (url, sum) = if req.body.iter().all(u8::is_ascii_whitespace) {
            ("", "")
        } else {
            let Ok(v) = json::parse(&req.body) else {
                return none(Response::error(400, "invalid json"));
            };
            parsed = v;
            let Some(o) = parsed.as_object() else {
                return none(Response::error(400, "body must be an object"));
            };
            let field = |k| o.get(k).and_then(Value::as_str).unwrap_or("");
            (field("url"), field("sha256"))
        };
        if url.is_empty() && !sum.is_empty() {
            return none(Response::error(400, "sha256 without url"));
        }
        if !url.is_empty() {
            if !url.contains("://") {
                return none(Response::error(400, "url must be absolute"));
            }
            if sum.len() != 64 || !sum.bytes().all(|c| c.is_ascii_hexdigit()) {
                return none(Response::error(400, "sha256 must be 64 hex characters"));
            }
        }
        let msg = if url.is_empty() {
            "restart requested: Hop hands its state to the kernel and starts again in its slot; the apps keep running"
        } else {
            "restart requested: the node fetches and verifies the new Hop, hands its state to the kernel and starts it in its slot; the apps keep running"
        };
        let effect = Effect::Restart {
            url: String::from(url),
            sha256: String::from(sum),
        };
        (reply(202, [("status", s(msg))]), effect)
    }
}

/// De vaste CORS-koppen van de agent-poort, zoals Go's `corsMiddleware`.
///
/// De browser van het dashboard stuurt `X-Hop-Auth` en bij een POST of
/// PATCH `Content-Type`; beide zijn geen "simple headers", dus de preflight
/// moet ze noemen. PATCH (de sleepvolgorde) en DELETE (een job weg) ook.
const CORS: [(&str, &str); 3] = [
    ("Access-Control-Allow-Origin", "*"),
    (
        "Access-Control-Allow-Methods",
        "GET, POST, DELETE, PATCH, OPTIONS",
    ),
    ("Access-Control-Allow-Headers", "Content-Type, X-Hop-Auth"),
];

/// De CORS-koppen voor een antwoord op `req` op de agent-poort.
///
/// Chrome Private Network Access: een publieke origin die een LAN-adres
/// haalt, krijgt de preflight alleen door als de server het toestaat; dat
/// ene koppel komt er alleen bij als de browser erom vraagt.
///
/// De adapter zet deze koppen op élk antwoord van de agent-poort, ook op
/// een doorgifte naar de leader en op een stroom (SSE, log-tail): zonder
/// `Access-Control-Allow-Origin` gooit de browser een geslaagd antwoord weg.
pub fn cors_headers(req: &Request) -> impl Iterator<Item = (&'static str, &'static str)> {
    let pna = req.header("Access-Control-Request-Private-Network") == Some("true");
    CORS.into_iter()
        .chain(pna.then_some(("Access-Control-Allow-Private-Network", "true")))
}

/// Zet de CORS-koppen van [`cors_headers`] op `resp`.
pub fn cors(req: &Request, resp: &mut Response) {
    for (k, v) in cors_headers(req) {
        resp.set_header(k, v);
    }
}

fn proxy(agent: &Agent, req: &Request) -> (Response, Effect) {
    let leader = agent.leader_addr();
    if leader.is_empty() {
        return (Response::error(503, "no leader available"), Effect::None);
    }
    if req.body.len() > PROXY_MAX_BODY {
        return (Response::error(413, "request body too large"), Effect::None);
    }
    let stream = req.path == "/v1/events"
        || (req.path.starts_with("/v1/agents/") && req.path.contains("/logs/"));
    (
        Response::empty(200),
        Effect::Proxy {
            leader: String::from(leader),
            stream,
        },
    )
}

fn leader(agent: &Agent) -> Response {
    let mut pairs = alloc::vec![("leader", s(agent.leader_addr()))];
    let exp = agent.lease_expires_at();
    if !exp.is_zero() {
        // Alleen de leader zelf kent zijn lease; een volger meldt alleen het adres.
        let mut t = String::new();
        if exp.write_rfc3339(&mut t).is_ok() {
            pairs.push(("lease_expires_at", s(&t)));
        }
    }
    reply(200, pairs)
}

fn capacity(agent: &Agent) -> Response {
    let c = agent.capacity();
    let mut attrs = types::de::ObjectBuilder::new();
    for (k, v) in agent.attributes() {
        if attrs.str(k, v).is_err() {
            return Response::empty(500);
        }
    }
    let mut pairs = alloc::vec![
        ("cpu_cores", Value::uint(u64::from(c.cpu_cores))),
        ("memory_bytes", Value::uint(c.memory_bytes)),
        ("cpu_used_shares", Value::int(c.cpu_used_shares)),
        ("memory_used_bytes", Value::uint(c.memory_used_bytes)),
        (
            "tasks_running",
            Value::uint(u64::try_from(c.tasks_running).unwrap_or(u64::MAX))
        ),
    ];
    if !agent.attributes().is_empty() {
        pairs.push(("attributes", attrs.build()));
    }
    reply(200, pairs)
}

fn tasks(agent: &Agent) -> Response {
    let mut list = Vec::new();
    for t in agent.tasks() {
        match t.to_value() {
            Ok(v) if list.try_reserve(1).is_ok() => list.push(v),
            _ => return Response::empty(500),
        }
    }
    Response::json(200, &Value::Array(list))
}

fn run(agent: &mut Agent, now: Nanos, pool_largest: Option<u64>, req: &Request) -> Response {
    if req.method != Method::Post {
        return Response::error(405, "method not allowed");
    }
    let replace = req.query_param("replace") == Some("1");
    let Ok(job) = Job::from_json(&req.body) else {
        return Response::error(400, "invalid json");
    };
    let name = job.name.clone();
    match agent.run(now, job, replace, pool_largest) {
        Ok(_) => reply(
            202,
            [
                ("status", s("accepted")),
                ("job", s(&name)),
                ("message", s("job accepted, starting in background")),
            ],
        ),
        Err(AgentError::AffinityMismatch) => Response::error(406, "affinity mismatch"),
        Err(AgentError::NoCapacity | AgentError::TooManyTasks | AgentError::TooManyJobs) => {
            Response::error(503, "insufficient capacity")
        }
        Err(_) => Response::error(500, "internal error"),
    }
}

fn count(key: &'static str, n: usize) -> Response {
    reply(
        200,
        [(key, Value::uint(u64::try_from(n).unwrap_or(u64::MAX)))],
    )
}

fn delete(agent: &mut Agent, now: Nanos, req: &Request, name: &str) -> Response {
    if req.method != Method::Delete {
        return Response::error(405, "method not allowed");
    }
    if name.is_empty() {
        return Response::error(400, "job name required");
    }
    count("deleted", agent.delete_job(now, name))
}

/// `/stop/{job}`: stopt de taken maar houdt de job (preemptie).
fn stop(agent: &mut Agent, req: &Request, name: &str) -> Response {
    if req.method != Method::Post {
        return Response::error(405, "method not allowed");
    }
    if name.is_empty() {
        return Response::error(400, "job name required");
    }
    count("stopped", agent.stop_job_tasks(name))
}

fn stop_task(agent: &mut Agent, req: &Request, id: &str) -> Response {
    if req.method != Method::Post {
        return Response::error(405, "method not allowed");
    }
    if id.is_empty() {
        return Response::error(400, "task id required");
    }
    if !agent.stop_task(id) {
        return Response::error(404, "task not found");
    }
    reply(200, [("stopped", s(id))])
}

fn logs(rest: &str) -> (Response, Effect) {
    let mut parts = rest.split('/');
    let (Some(id), Some(stream), None) = (parts.next(), parts.next(), parts.next()) else {
        return (
            Response::error(400, "usage: /logs/{taskID}/stdout or /logs/{taskID}/stderr"),
            Effect::None,
        );
    };
    let stream = match stream {
        "stdout" => LogStream::Stdout,
        "stderr" => LogStream::Stderr,
        _ => {
            return (
                Response::error(400, "stream must be stdout or stderr"),
                Effect::None,
            );
        }
    };
    let mut r = Response::empty(200);
    r.set_header("Content-Type", "text/event-stream");
    (
        r,
        Effect::Logs {
            task_id: String::from(id),
            stream,
        },
    )
}

#[cfg(test)]
mod flip_tests {
    //! `POST /flip`: alleen warm; en de routetabel.
    use super::*;
    use crate::Method;

    #[test]
    fn the_route_table_builds() {
        assert!(Routes::new(&ROUTES).is_built());
    }

    fn api_agent() -> Agent {
        Agent::new(agent::Settings {
            id: "flip-node".into(),
            ..agent::Settings::default()
        })
    }

    #[test]
    fn a_flip_is_warm_and_cold_is_refused() {
        let mut a = api_agent();
        let on = NodeApi::new(b"", true);
        let sum = "b".repeat(64);
        let body = |extra: &str| {
            alloc::format!(r#"{{"url":"http://10.0.2.2/k.flip","sha256":"{sum}"{extra}}}"#)
        };
        let ask = |a: &mut Agent, b: String| {
            on.handle(
                a,
                0,
                None,
                &Request::new(Method::Post, "/flip", b.as_bytes()),
            )
        };
        let (r, e) = ask(&mut a, body(""));
        assert_eq!(r.status, 202);
        assert!(matches!(e, Effect::Flip { .. }));
        let (r, e) = ask(&mut a, body(r#","cold":true"#));
        assert_eq!(r.status, 400, "no silent warm flip");
        assert_eq!(e, Effect::None);
    }
}
