use super::{
    Manager,
    thread::{Failure, fail},
};
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;
use croner::Cron;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

const LIMIT: usize = 256;

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub action: String,
    pub id: Option<String>,
    pub thread: Option<String>,
    pub session: Option<String>,
    pub name: Option<String>,
    pub prompt: Option<String>,
    pub cron: Option<String>,
    pub timezone: Option<String>,
    pub run_at: Option<DateTime<Utc>>,
    pub at: Option<DateTime<Utc>>,
    pub delay: Option<String>,
    pub enabled: Option<bool>,
    pub new_thread: Option<bool>,
    pub script: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Run {
    request_id: String,
    text: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct Automation {
    id: String,
    thread_id: String,
    session_id: String,
    name: String,
    prompt: String,
    cron: Option<String>,
    timezone: String,
    run_at: Option<DateTime<Utc>>,
    enabled: bool,
    next_run_at: Option<DateTime<Utc>>,
    last_run_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    last_request_id: Option<String>,
    last_run_state: Option<String>,
    failures: u32,
    runs: u64,
    origin: String,
    created_at: DateTime<Utc>,
    active: Option<Run>,
}

impl Automation {
    fn view(&self) -> Value {
        let mut value = serde_json::to_value(self).expect("automation serialization");
        value.as_object_mut().unwrap().remove("active");
        value["new_thread"] = json!(false);
        value["script"] = Value::Null;
        value
    }

    fn next(&self, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, String> {
        let zone: Tz = self.timezone.parse().map_err(|_| "Invalid timezone")?;
        if let Some(pattern) = &self.cron {
            if pattern.len() > 128 || pattern.split_whitespace().count() != 5 {
                return Err("Use a 5-field cron expression".into());
            }
            let cron: Cron = pattern.parse().map_err(|e| format!("Invalid cron: {e}"))?;
            let next = cron
                .find_next_occurrence(&now.with_timezone(&zone), false)
                .map_err(|e| format!("No next occurrence: {e}"))?;
            Ok(Some(next.with_timezone(&Utc)))
        } else {
            Ok(self.run_at)
        }
    }

    fn pause(&mut self, message: &str) {
        self.enabled = false;
        self.next_run_at = None;
        self.last_error = Some(message.into());
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Saved {
    version: u32,
    automations: BTreeMap<String, Automation>,
}

pub(super) struct Store {
    path: PathBuf,
    saved: Saved,
    writable: bool,
}

impl Store {
    pub(super) fn open(directory: &Path) -> io::Result<Self> {
        let path = directory.join("automations.json");
        let saved: Saved = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Saved {
                version: 1,
                automations: BTreeMap::new(),
            },
            Err(e) => return Err(e),
        };
        if saved.version != 1 || saved.automations.len() > LIMIT {
            return Err(io::Error::other("Unsupported automation store"));
        }
        Ok(Self {
            path,
            saved,
            writable: true,
        })
    }

    fn save(&mut self, next: Saved) -> io::Result<()> {
        if !self.writable {
            return Err(io::Error::other(
                "Loop store is unavailable; restart Core after repairing storage",
            ));
        }
        let temp = self.path.with_extension(format!("{}.tmp", Uuid::new_v4()));
        let result: io::Result<()> = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temp)?;
            file.write_all(&serde_json::to_vec(&next)?)?;
            file.sync_all()?;
            fs::rename(&temp, &self.path)?;
            File::open(self.path.parent().unwrap())?.sync_all()?;
            Ok(())
        })();
        let _ = fs::remove_file(temp);
        if let Err(error) = result {
            self.writable = false;
            return Err(error);
        }
        self.saved = next;
        Ok(())
    }

    fn put(&mut self, automation: Automation) -> io::Result<()> {
        let mut next = self.saved.clone();
        next.automations.insert(automation.id.clone(), automation);
        self.save(next)
    }
}

pub fn routes() -> Router<Arc<Manager>> {
    Router::new().route("/v1/automations", post(handle))
}

async fn handle(
    State(manager): State<Arc<Manager>>,
    input: Result<Json<Request>, axum::extract::rejection::JsonRejection>,
) -> Result<(StatusCode, Json<Value>), Failure> {
    let Json(input) = input.map_err(|error| bad(&error.body_text()))?;
    manager
        .automation(input, None)
        .map(|value| (StatusCode::ACCEPTED, Json(value)))
}

fn bad(message: &str) -> Failure {
    fail(StatusCode::BAD_REQUEST, message)
}
fn storage(error: io::Error) -> Failure {
    fail(
        StatusCode::SERVICE_UNAVAILABLE,
        &format!("Could not save loops: {error}"),
    )
}

fn time(input: &Request, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, Failure> {
    if let Some(delay) = &input.delay {
        let words: Vec<_> = delay.split_whitespace().collect();
        let seconds = match words.as_slice() {
            [count, unit] => count.parse::<i64>().ok().filter(|n| *n > 0).and_then(|n| {
                n.checked_mul(match *unit {
                    "seconds" => 1,
                    "minutes" => 60,
                    "hours" => 3600,
                    "days" => 86400,
                    _ => 0,
                })
            }),
            _ => None,
        }
        .filter(|n| *n > 0 && *n <= 10 * 366 * 86400)
        .ok_or_else(|| bad("Invalid delay (maximum ten years)"))?;
        return now
            .checked_add_signed(Duration::seconds(seconds))
            .map(Some)
            .ok_or_else(|| bad("Invalid delay"));
    }
    Ok(input.run_at.or(input.at))
}

impl Manager {
    pub(super) fn automation(
        &self,
        input: Request,
        caller: Option<&str>,
    ) -> Result<Value, Failure> {
        if self.is_stopping() {
            return Err(fail(StatusCode::CONFLICT, "Core is stopping"));
        }
        let now = Utc::now();
        if input.new_thread == Some(true) || input.script.as_ref().is_some_and(|s| !s.is_empty()) {
            return Err(bad(
                "Self-hosted loops currently support --same-thread prompts only; fresh threads and preflight scripts are not supported",
            ));
        }
        for (value, max, label) in [(&input.name, 200, "name"), (&input.prompt, 32000, "prompt")] {
            if value
                .as_ref()
                .is_some_and(|v| v.trim().is_empty() || v.len() > max)
            {
                return Err(bad(&format!("Invalid {label} (1-{max} bytes required)")));
            }
        }
        let schedules = [
            input.cron.is_some(),
            input.run_at.is_some(),
            input.at.is_some(),
            input.delay.is_some(),
        ]
        .into_iter()
        .filter(|v| *v)
        .count();
        if schedules > 1 {
            return Err(bad("Use exactly one schedule: cron, run_at, at, or delay"));
        }
        let run_at = time(&input, now)?;
        if run_at.is_some_and(|at| at <= now) {
            return Err(bad("One-time runs must be in the future"));
        }
        let mut store = self.scheduler.lock().unwrap();
        if !store.writable {
            return Err(fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "Loop store is unavailable; restart Core after repairing storage",
            ));
        }
        let visible = |row: &Automation| caller.is_none_or(|session| row.session_id == session);
        if input.action == "list" {
            return Ok(
                json!({"scheduler":"core", "automations":store.saved.automations.values().filter(|a| visible(a) && input.thread.as_ref().is_none_or(|t| &a.thread_id == t) && input.session.as_ref().is_none_or(|s| &a.session_id == s)).map(Automation::view).collect::<Vec<_>>()}),
            );
        }
        if input.action == "archived" {
            let session = caller
                .or(input.session.as_deref())
                .ok_or_else(|| bad("Session required"))?;
            let mut next = store.saved.clone();
            for row in next
                .automations
                .values_mut()
                .filter(|a| a.session_id == session)
            {
                row.pause("Paused because its thread was archived");
                if row
                    .active
                    .as_ref()
                    .is_some_and(|r| self.receipt(session, &r.request_id).is_none())
                {
                    row.active = None;
                    row.last_run_state = Some("cancelled".into());
                }
            }
            store.save(next).map_err(storage)?;
            return Ok(json!({"ok":true}));
        }
        let mut row = if input.action == "create" {
            if store.saved.automations.len() >= LIMIT {
                return Err(bad("Core supports at most 256 loops"));
            }
            let session = caller
                .or(input.session.as_deref())
                .ok_or_else(|| bad("An existing Core session is required"))?;
            let local = self.local.lock().unwrap();
            let target = local
                .sessions
                .get(session)
                .ok_or_else(|| bad("Core session not found"))?;
            if target.archived
                || matches!(target.state.as_str(), "closed" | "failed" | "process_lost")
            {
                return Err(bad("Use an open Core thread"));
            }
            if schedules != 1 {
                return Err(bad("A schedule is required"));
            }
            Automation {
                id: Uuid::new_v4().to_string(),
                thread_id: input.thread.clone().unwrap_or_else(|| session.into()),
                session_id: session.into(),
                name: input.name.clone().ok_or_else(|| bad("Name required"))?,
                prompt: input.prompt.clone().ok_or_else(|| bad("Prompt required"))?,
                cron: input.cron.clone(),
                timezone: input.timezone.clone().unwrap_or_else(|| "UTC".into()),
                run_at,
                enabled: input.enabled.unwrap_or(true),
                next_run_at: None,
                last_run_at: None,
                last_error: None,
                last_request_id: None,
                last_run_state: None,
                failures: 0,
                runs: 0,
                origin: if caller.is_some() { "agent" } else { "app" }.into(),
                created_at: now,
                active: None,
            }
        } else {
            let id = input.id.as_ref().ok_or_else(|| bad("Loop ID required"))?;
            store
                .saved
                .automations
                .get(id)
                .filter(|a| visible(a))
                .cloned()
                .ok_or_else(|| fail(StatusCode::NOT_FOUND, "Loop not found"))?
        };
        match input.action.as_str() {
            "create" | "update" | "pause" | "resume" => {
                if let Some(name) = input.name {
                    row.name = name;
                }
                if let Some(prompt) = input.prompt {
                    row.prompt = prompt;
                }
                if let Some(cron) = input.cron {
                    row.cron = Some(cron);
                    row.run_at = None;
                }
                if let Some(at) = run_at {
                    row.run_at = Some(at);
                    row.cron = None;
                }
                let timezone_changed = input.timezone.is_some();
                if let Some(zone) = input.timezone {
                    row.timezone = zone;
                }
                let enabled = match input.action.as_str() {
                    "pause" => Some(false),
                    "resume" => Some(true),
                    _ => input.enabled,
                };
                if let Some(on) = enabled {
                    row.enabled = on;
                }
                let next = row.next(now).map_err(|e| bad(&e))?;
                if row.enabled && row.cron.is_none() && row.run_at.is_none_or(|at| at <= now) {
                    return Err(bad("Set a future time to resume a completed one-time loop"));
                }
                if input.action == "create"
                    || schedules > 0
                    || timezone_changed
                    || enabled.is_some()
                {
                    row.next_run_at = if row.enabled { next } else { None };
                }
                if enabled == Some(true) {
                    row.failures = 0;
                    row.last_error = None;
                }
                if enabled == Some(false)
                    && row
                        .active
                        .as_ref()
                        .is_some_and(|r| self.receipt(&row.session_id, &r.request_id).is_none())
                {
                    row.active = None;
                    row.last_run_state = Some("cancelled".into());
                }
            }
            "run" => {
                if row.active.is_some() {
                    return Err(fail(
                        StatusCode::CONFLICT,
                        "This loop already has a pending run",
                    ));
                }
                row.active = Some(Run {
                    request_id: format!("loop_{}", Uuid::new_v4().simple()),
                    text: row.prompt.clone(),
                });
                row.last_run_at = Some(now);
                row.last_request_id = row.active.as_ref().map(|r| r.request_id.clone());
                row.last_run_state = Some("pending".into());
                row.runs += 1;
            }
            "delete" => {
                let mut next = store.saved.clone();
                next.automations.remove(&row.id);
                store.save(next).map_err(storage)?;
                return Ok(json!({"ok":true}));
            }
            _ => return Err(bad("Unknown loop action")),
        }
        let view = row.view();
        store.put(row).map_err(storage)?;
        Ok(json!({"scheduler":"core", "automation":view}))
    }

    pub fn start_scheduler(self: &Arc<Self>) {
        let manager = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last_error = None;
            loop {
                interval.tick().await;
                let Some(manager) = manager.upgrade() else {
                    break;
                };
                if manager.is_stopping() {
                    break;
                }
                let error = manager
                    .tick_automations(Utc::now())
                    .err()
                    .map(|e| e.to_string());
                if error != last_error
                    && let Some(error) = &error
                {
                    eprintln!("Loop scheduler: {error}");
                }
                last_error = error;
            }
        });
    }

    fn tick_automations(self: &Arc<Self>, now: DateTime<Utc>) -> io::Result<()> {
        let mut store = self.scheduler.lock().unwrap();
        if !store.writable {
            return Err(io::Error::other(
                "Loop store is unavailable; restart Core after repairing storage",
            ));
        }
        let rows: Vec<_> = store.saved.automations.values().cloned().collect();
        for mut row in rows {
            if row.active.is_none() && (!row.enabled || row.next_run_at.is_none_or(|at| at > now)) {
                continue;
            }
            let local = self.local.lock().unwrap();
            let Some(session) = local.sessions.get(&row.session_id) else {
                row.pause("Core session is missing");
                row.active = None;
                drop(local);
                store.put(row)?;
                continue;
            };
            if let Some(run) = &row.active
                && let Some(receipt) = session.receipts.get(&run.request_id)
            {
                let cancelled = session
                    .prompts
                    .get(&run.request_id)
                    .is_some_and(|p| p.cancelled);
                let state = if cancelled {
                    "cancelled"
                } else {
                    receipt.state.as_str()
                };
                if !matches!(
                    state,
                    "completed"
                        | "failed"
                        | "interrupted"
                        | "unknown"
                        | "unknown_after_restart"
                        | "cancelled"
                ) {
                    continue;
                }
                row.last_run_state = Some(state.into());
                if state == "completed" {
                    row.failures = 0;
                    row.last_error = None;
                } else {
                    row.failures = row.failures.saturating_add(1);
                    row.last_error = Some(
                        receipt
                            .error
                            .clone()
                            .unwrap_or_else(|| format!("Run ended: {state}")),
                    );
                    if row.failures >= 3 || matches!(state, "unknown" | "unknown_after_restart") {
                        row.enabled = false;
                        row.next_run_at = None;
                    }
                }
                row.active = None;
                drop(local);
                store.put(row)?;
                continue;
            }
            if session.archived
                || matches!(session.state.as_str(), "closed" | "failed" | "process_lost")
            {
                row.pause("Paused because its Core thread is archived or unavailable");
                row.active = None;
                drop(local);
                store.put(row)?;
                continue;
            }
            if local.draining
                || self.storage.blocks()
                || session.queue_paused
                || session.storage_paused
                || session.busy()
                || !matches!(session.state.as_str(), "idle" | "sleeping")
            {
                continue;
            }
            drop(local);
            if row.active.is_none() {
                row.next_run_at = if row.cron.is_some() {
                    match row.next(now) {
                        Ok(next) => next,
                        Err(error) => {
                            row.pause(&error);
                            store.put(row)?;
                            continue;
                        }
                    }
                } else {
                    row.enabled = false;
                    None
                };
                row.active = Some(Run {
                    request_id: format!("loop_{}", Uuid::new_v4().simple()),
                    text: row.prompt.clone(),
                });
                row.last_run_at = Some(now);
                row.last_request_id = row.active.as_ref().map(|r| r.request_id.clone());
                row.last_run_state = Some("pending".into());
                row.runs += 1;
                store.put(row.clone())?;
            }
            let run = row.active.as_ref().unwrap();
            match self.command(
                &row.session_id,
                run.request_id.clone(),
                "prompt",
                json!({"text":run.text}),
            ) {
                Ok(_) => {}
                Err(error) => {
                    let message = format!("Dispatch deferred: {}", error.message());
                    if row.last_error.as_ref() != Some(&message) {
                        row.last_error = Some(message);
                        store.put(row)?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Config, runtime, session::Session};

    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn fixture() -> (Directory, Arc<Manager>) {
        let directory =
            Directory(std::env::temp_dir().join(format!("core-scheduler-{}", Uuid::new_v4())));
        fs::create_dir(&directory.0).unwrap();
        let config = Config {
            listen: "127.0.0.1:0".parse().unwrap(),
            token: "fixture".into(),
            state_dir: directory.0.clone(),
            repository: directory.0.clone(),
            api_url: None,
            store: "fixture".into(),
            instance: None,
            account_home: directory.0.clone(),
            default_harness: runtime::Kind::Codex,
            harnesses: BTreeMap::new(),
            storage: None,
            rpc_timeout: std::time::Duration::from_secs(1),
            diagnostic_upload: std::time::Duration::from_secs(60),
        };
        let manager = Manager::open(config).unwrap();
        for id in ["cr_a", "cr_b"] {
            manager.local.lock().unwrap().sessions.insert(
                id.into(),
                Session {
                    session_id: id.into(),
                    state: "idle".into(),
                    ..Session::default()
                },
            );
        }
        (directory, manager)
    }
    fn request(value: Value) -> Request {
        serde_json::from_value(value).unwrap()
    }
    fn create(manager: &Manager, caller: &str) -> String {
        manager.automation(request(json!({"action":"create","name":"test","prompt":"inspect","cron":"0 9 * * 1-5","timezone":"Europe/Zurich"})), Some(caller)).unwrap()["automation"]["id"].as_str().unwrap().into()
    }

    #[tokio::test]
    async fn caller_scope_is_enforced_for_every_mutation() {
        let (_dir, manager) = fixture();
        let id = create(&manager, "cr_a");
        create(&manager, "cr_b");
        for action in ["update", "pause", "resume", "run", "delete"] {
            let error = manager
                .automation(
                    request(json!({"action":action,"id":id,"enabled":false})),
                    Some("cr_b"),
                )
                .unwrap_err();
            assert_eq!(error.0, StatusCode::NOT_FOUND);
        }
        let listed = manager
            .automation(request(json!({"action":"list"})), Some("cr_a"))
            .unwrap();
        assert_eq!(listed["automations"].as_array().unwrap().len(), 1);
        manager
            .automation(
                request(json!({"action":"archived","session":"cr_a"})),
                Some("cr_b"),
            )
            .unwrap();
        assert!(manager.scheduler.lock().unwrap().saved.automations[&id].enabled);
    }

    #[tokio::test]
    async fn cron_follows_timezone_across_both_dst_changes() {
        let (_dir, manager) = fixture();
        let id = create(&manager, "cr_a");
        let store = manager.scheduler.lock().unwrap();
        let row = &store.saved.automations[&id];
        for (start, expected) in [
            ("2026-03-27T10:00:00Z", "2026-03-30T07:00:00Z"),
            ("2026-10-23T10:00:00Z", "2026-10-26T08:00:00Z"),
        ] {
            let start = start.parse::<DateTime<Utc>>().unwrap();
            assert_eq!(
                row.next(start).unwrap().unwrap(),
                expected.parse::<DateTime<Utc>>().unwrap()
            );
        }
    }

    #[tokio::test]
    async fn pending_run_identity_and_prompt_survive_reload_and_edit() {
        let (dir, manager) = fixture();
        let id = create(&manager, "cr_a");
        manager
            .automation(request(json!({"action":"run","id":id})), None)
            .unwrap();
        manager
            .automation(
                request(json!({"action":"update","id":id,"prompt":"new prompt"})),
                None,
            )
            .unwrap();
        let reloaded = Store::open(&dir.0).unwrap();
        let old = manager.scheduler.lock().unwrap();
        let active = reloaded.saved.automations[&id].active.as_ref().unwrap();
        assert_eq!(active.text, "inspect");
        assert_eq!(
            active.request_id,
            old.saved.automations[&id]
                .active
                .as_ref()
                .unwrap()
                .request_id
        );
        assert_eq!(reloaded.saved.automations[&id].prompt, "new prompt");
    }

    #[tokio::test]
    async fn failed_storage_stops_mutations_and_execution() {
        let (dir, manager) = fixture();
        let id = create(&manager, "cr_a");
        fs::remove_file(dir.0.join("automations.json")).unwrap();
        fs::create_dir(dir.0.join("automations.json")).unwrap();
        assert!(
            manager
                .automation(request(json!({"action":"run","id":id})), None)
                .is_err()
        );
        assert!(manager.tick_automations(Utc::now()).is_err());
        assert!(!manager.scheduler.lock().unwrap().writable);
        assert!(
            manager.local.lock().unwrap().sessions["cr_a"]
                .receipts
                .is_empty()
        );
    }
}
