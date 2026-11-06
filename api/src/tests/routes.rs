//! De routetabellen van de agent- en de leader-API vastgelegd: per methode
//! en pad de status, de body, het inhoudstype en het effect, met en zonder
//! sleutel. Vastgelegd op de routering met de hand (06-11-2026), vóór de
//! overstap op `leanhttp::Mux`; die overstap mocht hier geen byte aan
//! veranderen. `routes.golden` is de verwachting.

use super::*;

const METHODS: [Method; 6] = [
    Method::Get,
    Method::Post,
    Method::Delete,
    Method::Patch,
    Method::Options,
    // HEAD, PUT en de rest: `Method::parse` maakt er `Other` van.
    Method::Other,
];

const NODE_PATHS: &[&str] = &[
    "/",
    "/health",
    "/health/",
    "/leader",
    "/leader/",
    "/v1",
    "/v1/",
    "/v1/agents",
    "/v1/agents/",
    "/v1/agents/x",
    "/v1/agents/x/logs/t/stdout",
    "/v1/jobs",
    "/v1/jobs/",
    "/v1/jobs/a/b",
    "/v1/status",
    "/v1/status/",
    "/v1/tasks",
    "/v1/events",
    "/v1/eventsx",
    "/logs",
    "/logs/",
    "/logs/t",
    "/logs/t/",
    "/logs/t/stdout",
    "/logs/t/stderr/",
    "/logs/t/x",
    "/logs/t/stdout/more",
    "/capacity",
    "/capacity/",
    "/tasks",
    "/tasks/",
    "/run",
    "/run/",
    "/flip",
    "/restart",
    "/delete",
    "/delete/",
    "/delete/j",
    "/delete/j/",
    "/delete/a/b",
    "/stop",
    "/stop/",
    "/stop/j",
    "/stop/a/b",
    "/stop-task",
    "/stop-task/",
    "/stop-task/t1",
    "/stop-task/a/b",
    "/stopx",
    "/unknown",
    "/a/b/c",
];

const LEADER_PATHS: &[&str] = &[
    "/",
    "/health",
    "/health/",
    "/leader",
    "/v1",
    "/v1/tasks",
    "/v1/tasks/",
    "/v1/jobs",
    "/v1/jobs/",
    "/v1/jobs/status",
    "/v1/jobs/j/status",
    "/v1/jobs/none/status",
    "/v1/jobs/a/b/status",
    "/v1/jobs/j/status/",
    "/v1/jobs/j",
    "/v1/jobs/j/",
    "/v1/jobs/a/b",
    "/v1/jobs/priority",
    "/v1/jobs/j/priority",
    "/v1/jobs/a/b/priority",
    "/v1/jobs/j/priority/priority",
    "/v1/jobs/j/priority/",
    "/v1/events",
    "/v1/events/",
    "/v1/agents",
    "/v1/agents/",
    "/v1/agents/a1",
    "/v1/agents/a1/",
    "/v1/agents/a/b",
    "/v1/agents/a1/capacity",
    "/v1/agents/a1/capacity/",
    "/v1/agents/nope/capacity",
    "/v1/agents/a1/logs",
    "/v1/agents/a1/logs/",
    "/v1/agents/a1/logs/t",
    "/v1/agents/a1/logs/t/stdout",
    "/v1/agents/a1/logs/t/stderr?follow=0",
    "/v1/agents/a1/logs/t/x",
    "/v1/agents/a1/logs/t/stdout/x",
    "/v1/agents/nope/logs/t/stdout",
    "/v1/heartbeat",
    "/v1/heartbeat/",
    "/v1/notify",
    "/v1/status",
    "/v1/statusx",
    "/x",
];

/// De body van elk verzoek: een geldige job, zodat POST en PATCH verder
/// komen dan "invalid json".
const BODY: &str =
    r#"{"name":"j","command":"x","priority":3,"id":"a2","endpoint":"http://10.0.0.3:8080"}"#;

fn line(key: &str, m: Method, path: &str, r: &Response, effect: &str) -> String {
    alloc::format!(
        "{key} {} {path} -> {} {} {} {effect}\n",
        m.as_str(),
        r.status,
        r.header("Content-Type").unwrap_or("-"),
        String::from_utf8_lossy(&r.body),
    )
}

fn fake() -> FakeCluster {
    let mut c = FakeCluster::default();
    c.register_agent(T0, "a1", "http://10.0.0.2:8080", "v", &[]);
    c.jobs.push(types::Job {
        name: "j".into(),
        ..types::Job::default()
    });
    c.placed_on.push(("a1".into(), "j".into()));
    c
}

fn table() -> String {
    let mut out = String::new();
    for key in ["", "k"] {
        for path in NODE_PATHS {
            for m in METHODS {
                let mut a = agent();
                a.set_leader_addr("10.0.0.1:9080");
                let api = NodeApi::new(key.as_bytes(), true);
                let req = Request::new(m, path, BODY.as_bytes());
                let (r, e) = api.handle(&mut a, T0, None, &req);
                out.push_str(&line(
                    &alloc::format!("node[{key}]"),
                    m,
                    path,
                    &r,
                    &alloc::format!("{e:?}"),
                ));
            }
        }
        for path in LEADER_PATHS {
            for m in METHODS {
                let mut c = fake();
                let api = LeaderApi::new(key.as_bytes(), "test-cluster");
                let req = Request::new(m, path, BODY.as_bytes());
                let (r, e) = api.handle(&mut c, T0, &req);
                let tail = alloc::format!("{e:?} jobs={} agents={}", c.jobs.len(), c.agents.len());
                out.push_str(&line(&alloc::format!("leader[{key}]"), m, path, &r, &tail));
            }
        }
    }
    out
}

#[test]
fn the_route_tables_answer_as_before() {
    let got = table();
    let want = include_str!("routes.golden");
    for (i, (g, w)) in got.lines().zip(want.lines()).enumerate() {
        assert_eq!(g, w, "line {}", i + 1);
    }
    assert_eq!(got.lines().count(), want.lines().count());
}
