# Server Foundation: Persistent Views + Schema Catalog — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two cqserver admin capabilities — `GET /admin/catalog` (all topics + views with their fields/types) and `POST /admin/views` (create a materialized view at runtime, persisted so it is recreated on restart) — so the example app's admin screen and query builder can author live views.

**Architecture:** Reuse the existing boot-time `init_view(cfg, topics, registry)` to stand a view up live at runtime, then persist its `ViewEntry` to a dedicated `runtime_views.toml` (separate from the hand-authored `cqserver.toml`). On boot, merge `runtime_views.toml` into `server_config.views` before the view-init loop so persisted views are recreated. No live teardown (deferred). The catalog reads each topic's `Schema`; a shared view-name set tags `kind`.

**Tech Stack:** Rust, axum (admin HTTP), serde/toml, dashmap; cq-e2e-tests harness (`start_server`, `start_server_with`, `stop_keeping_dir`, `restart_kept`) + reqwest + cq_client for e2e.

This plan is **Sub-project 1** of the design in `docs/superpowers/specs/2026-05-27-server-driven-examples-rewrite-design.md`. Sub-projects 2 (admin screen) and 3 (builder + examples) get their own plans after this lands, so they can target real endpoints.

---

## File Structure

- **Modify** `crates/cq-server/src/config.rs` — add `Serialize` to `ViewEntry`; add `RuntimeViewsFile` struct + `load_runtime_views` / `persist_runtime_view` helpers; add `runtime_views_path` field to `ServerConfig`.
- **Modify** `crates/cq-server/src/main.rs` — make `init_view` `pub(crate)`; merge runtime views at boot; add `view_names` + `runtime_views_path` to `AdminState`.
- **Modify** `crates/cq-server/src/admin.rs` — add `view_names` + `runtime_views_path` fields to `AdminState`; add `get_catalog` and `create_view` handlers + routes.
- **Create** `crates/cq-e2e-tests/tests/admin_views_runtime.rs` — e2e for catalog, create→live, restart→recreated, invalid-SQL rejection.

---

## Task 1: Runtime-views file load/save helpers (config.rs)

**Files:**
- Modify: `crates/cq-server/src/config.rs:3` (imports), `:625-646` (ViewEntry derive), add new structs/fns near the bottom of the file.
- Test: inline `#[cfg(test)]` in `crates/cq-server/src/config.rs`.

- [ ] **Step 1: Write the failing test**

Add at the end of `crates/cq-server/src/config.rs`:

```rust
#[cfg(test)]
mod runtime_views_tests {
    use super::*;

    fn temp_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "cq_rt_views_{}_{}.toml",
            std::process::id(),
            tag
        ))
    }

    fn sample(name: &str) -> ViewEntry {
        ViewEntry {
            name: name.into(),
            source: "/positions".into(),
            sql: "SELECT sector, COUNT(*) AS n FROM t GROUP BY sector".into(),
            initial_capacity: 10_000,
            tap_capacity: 1024,
        }
    }

    #[test]
    fn load_missing_file_returns_empty() {
        let p = temp_path("missing");
        let _ = std::fs::remove_file(&p);
        let views = load_runtime_views(&p).expect("load");
        assert!(views.is_empty());
    }

    #[test]
    fn persist_then_load_roundtrips() {
        let p = temp_path("roundtrip");
        let _ = std::fs::remove_file(&p);
        persist_runtime_view(&p, &sample("/v_a")).expect("persist a");
        persist_runtime_view(&p, &sample("/v_b")).expect("persist b");
        let views = load_runtime_views(&p).expect("load");
        assert_eq!(views.len(), 2);
        assert_eq!(views[0].name, "/v_a");
        assert_eq!(views[1].name, "/v_b");
        assert_eq!(views[0].source, "/positions");
        let _ = std::fs::remove_file(&p);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p cq-server runtime_views_tests 2>&1 | tail -20`
Expected: FAIL — `load_runtime_views` / `persist_runtime_view` not found.

- [ ] **Step 3: Implement the helpers**

In `crates/cq-server/src/config.rs`, change the import at the top:

```rust
use serde::{Deserialize, Serialize};
```

Add `Serialize` to the `ViewEntry` derive (currently `#[derive(Debug, Clone, Deserialize)]`):

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ViewEntry {
```

Add near the bottom of the file (before the `#[cfg(test)]` block):

```rust
/// On-disk shape of the runtime-views file. A plain `[[views]]` array
/// of `ViewEntry`, identical to the `views` section of cqserver.toml,
/// but written/owned by the server (admin-created views) rather than
/// hand-authored. Kept separate so programmatic rewrites never disturb
/// the operator's cqserver.toml.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct RuntimeViewsFile {
    #[serde(default)]
    pub views: Vec<ViewEntry>,
}

/// Read the runtime-views file. Returns an empty vec if the file is
/// absent or blank (the common first-run case).
pub fn load_runtime_views(
    path: &std::path::Path,
) -> Result<Vec<ViewEntry>, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = std::fs::read_to_string(path)?;
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    let parsed: RuntimeViewsFile = toml::from_str(&raw)?;
    Ok(parsed.views)
}

/// Append a view to the runtime-views file. Read-modify-write the whole
/// file, then rename a temp file over the target so a crash mid-write
/// never leaves a half-written config.
pub fn persist_runtime_view(
    path: &std::path::Path,
    entry: &ViewEntry,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut existing = load_runtime_views(path)?;
    existing.push(entry.clone());
    let file = RuntimeViewsFile { views: existing };
    let toml_text = toml::to_string_pretty(&file)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, toml_text)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p cq-server runtime_views_tests 2>&1 | tail -20`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/cq-server/src/config.rs
git commit -m "feat(config): runtime-views file load/persist helpers"
```

---

## Task 2: ServerConfig.runtime_views_path + boot merge (config.rs, main.rs)

**Files:**
- Modify: `crates/cq-server/src/config.rs:6-57` (ServerConfig fields), `:656-699` (Default impl).
- Modify: `crates/cq-server/src/main.rs:83-86` (mut binding + merge).

- [ ] **Step 1: Add the config field**

In `crates/cq-server/src/config.rs`, inside `pub struct ServerConfig`, add after the `views` field (around line 25):

```rust
    /// Path to the server-owned runtime-views file (admin-created
    /// views). Defaults to `<config_dir>/runtime_views.toml` when
    /// unset. Loaded + merged into `views` at boot so admin-created
    /// views survive restart.
    #[serde(default)]
    pub runtime_views_path: Option<String>,
```

In `impl Default for ServerConfig`, add the field to the returned struct (next to `views: Vec::new(),` around line 689):

```rust
            runtime_views_path: None,
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p cq-server 2>&1 | tail -20`
Expected: builds (no missing-field error in the Default impl).

- [ ] **Step 3: Merge runtime views at boot**

In `crates/cq-server/src/main.rs`, change the config-load binding (lines 83-86) to make `server_config` mutable and merge runtime views immediately after:

```rust
    let config_override = parse_config_arg();
    let (mut server_config, config_dir, raw_config_toml) = match config_override {
        Some(path) => config::load_config_from_with_raw(&path)?,
        None => config::load_config_with_raw()?,
    };

    // Resolve the runtime-views file (admin-created views) and merge
    // it into the declared views BEFORE validation + the init_view
    // loop, so persisted views are recreated on restart exactly like
    // hand-authored ones.
    let runtime_views_path: std::path::PathBuf = server_config
        .runtime_views_path
        .clone()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| config_dir.join("runtime_views.toml"));
    match config::load_runtime_views(&runtime_views_path) {
        Ok(rt_views) => {
            if !rt_views.is_empty() {
                server_config.views.extend(rt_views);
            }
        }
        Err(e) => {
            eprintln!(
                "warning: failed to load runtime views from {}: {e}",
                runtime_views_path.display()
            );
        }
    }
```

(Note: `tracing` is not yet installed at this point in `main`, which is why the warning goes to `eprintln!` like the surrounding bootstrap code.)

- [ ] **Step 4: Verify it compiles**

Run: `cargo build -p cq-server 2>&1 | tail -20`
Expected: builds. `runtime_views_path` is now in scope for later tasks.

- [ ] **Step 5: Commit**

```bash
git add crates/cq-server/src/config.rs crates/cq-server/src/main.rs
git commit -m "feat(server): merge runtime_views.toml into views at boot"
```

---

## Task 3: init_view pub(crate) + AdminState view-set/path wiring (main.rs, admin.rs)

**Files:**
- Modify: `crates/cq-server/src/main.rs:818` (init_view visibility), `:304-323` (AdminState construction).
- Modify: `crates/cq-server/src/admin.rs:34-57` (AdminState struct).

- [ ] **Step 1: Make init_view callable from admin.rs**

In `crates/cq-server/src/main.rs`, change the `init_view` signature line (818) from:

```rust
fn init_view(
```
to:
```rust
pub(crate) fn init_view(
```

- [ ] **Step 2: Add the new AdminState fields**

In `crates/cq-server/src/admin.rs`, add to `pub struct AdminState` (after `raw_config_toml`, around line 56):

```rust
    /// Live set of canonical view names (boot-declared + admin-created).
    /// `GET /admin/catalog` uses this to tag each topic as
    /// `topic` vs `view`. `POST /admin/views` inserts into it on
    /// success.
    pub view_names: Arc<dashmap::DashSet<String>>,
    /// Path to the runtime-views file that `POST /admin/views` appends
    /// to so admin-created views survive restart.
    pub runtime_views_path: Arc<std::path::PathBuf>,
```

- [ ] **Step 3: Populate the fields at construction**

In `crates/cq-server/src/main.rs`, just before the `let admin_state = AdminState {` block (line 304), build the view-name set from the merged views:

```rust
    let view_names: Arc<dashmap::DashSet<String>> = Arc::new(dashmap::DashSet::new());
    for v in &server_config.views {
        view_names.insert(cq_core::topic::canonicalize_topic(&v.name));
    }
```

Then add these two fields inside the `AdminState { ... }` literal (after `raw_config_toml: Arc::new(raw_config_toml),`):

```rust
        view_names: view_names.clone(),
        runtime_views_path: Arc::new(runtime_views_path.clone()),
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo build -p cq-server 2>&1 | tail -25`
Expected: builds. (If `dashmap::DashSet` is unresolved, confirm `dashmap` is a dependency — it is, `DashMap` is already used in this file.)

- [ ] **Step 5: Commit**

```bash
git add crates/cq-server/src/main.rs crates/cq-server/src/admin.rs
git commit -m "feat(admin): wire view-name set + runtime-views path into AdminState"
```

---

## Task 4: GET /admin/catalog (admin.rs + e2e)

**Files:**
- Modify: `crates/cq-server/src/admin.rs:76-96` (route table), add `get_catalog` handler near `get_topics`.
- Create: `crates/cq-e2e-tests/tests/admin_views_runtime.rs`.

- [ ] **Step 1: Write the failing e2e test**

Create `crates/cq-e2e-tests/tests/admin_views_runtime.rs`:

```rust
//! Sub-project 1 e2e — schema catalog + runtime view creation.

use cq_e2e_tests::{start_server_with, ServerOpts, TopicSpec, ViewSpec};
use serde_json::Value;

#[tokio::test]
async fn catalog_lists_topics_and_views_with_columns() {
    let topic = TopicSpec::new("/positions", "position_id").with_inline_columns([
        ("position_id", "string"),
        ("sector", "string"),
        ("mv", "double"),
    ]);
    let opts = ServerOpts {
        views: vec![ViewSpec::new(
            "/v_by_sector",
            "/positions",
            "SELECT sector, COUNT(*) AS n FROM t GROUP BY sector",
        )],
        ..ServerOpts::default()
    };
    let server = start_server_with(vec![topic], opts).await;

    let url = format!("{}/admin/catalog", server.admin_url());
    let body: Vec<Value> = reqwest::get(&url)
        .await
        .expect("catalog GET")
        .json()
        .await
        .expect("catalog json");

    let pos = body
        .iter()
        .find(|e| e["name"] == "/positions")
        .expect("positions present");
    assert_eq!(pos["kind"], "topic");
    let cols: Vec<&str> = pos["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert!(cols.contains(&"sector"), "columns: {cols:?}");

    let view = body
        .iter()
        .find(|e| e["name"] == "/v_by_sector")
        .expect("view present");
    assert_eq!(view["kind"], "view");
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p cq-e2e-tests --test admin_views_runtime catalog_lists 2>&1 | tail -25`
Expected: FAIL — `/admin/catalog` returns 404, so `.json()` errors or the asserts fail.

- [ ] **Step 3: Add the handler + route**

In `crates/cq-server/src/admin.rs`, add the route in `start_admin_server` (in the `.route(...)` chain near line 92):

```rust
        .route("/admin/catalog", get(get_catalog))
```

Add the handler near `get_topics` (after line 190):

```rust
/// `GET /admin/catalog` — every topic + view with its column list and
/// types. Feeds the admin "create view" screen and the query builder's
/// schema/field catalog. `kind` distinguishes views (in `view_names`)
/// from regular topics; both live in the same `topics` map.
async fn get_catalog(State(s): State<AdminState>) -> impl IntoResponse {
    let mut out: Vec<serde_json::Value> = Vec::with_capacity(s.topics.len());
    for e in s.topics.iter() {
        let name = e.key().clone();
        let schema = e.value().schema();
        let columns: Vec<serde_json::Value> = (0..schema.column_count())
            .map(|i| {
                serde_json::json!({
                    "name": schema.column_name(i),
                    // ColumnType derives Serialize with rename_all =
                    // "lowercase" → "double" | "string" | ...
                    "type": schema.column_type(i),
                })
            })
            .collect();
        let kind = if s.view_names.contains(&name) {
            "view"
        } else {
            "topic"
        };
        out.push(serde_json::json!({
            "name": name,
            "kind": kind,
            "columns": columns,
        }));
    }
    Json(serde_json::Value::Array(out))
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p cq-e2e-tests --test admin_views_runtime catalog_lists 2>&1 | tail -25`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/cq-server/src/admin.rs crates/cq-e2e-tests/tests/admin_views_runtime.rs
git commit -m "feat(admin): GET /admin/catalog — topics + views with field types"
```

---

## Task 5: POST /admin/views (admin.rs + e2e)

**Files:**
- Modify: `crates/cq-server/src/admin.rs` — route line for `/admin/views`, add `CreateViewRequest` + `create_view`.
- Modify: `crates/cq-e2e-tests/tests/admin_views_runtime.rs` — add 3 tests.

- [ ] **Step 1: Write the failing e2e tests**

Append to `crates/cq-e2e-tests/tests/admin_views_runtime.rs`:

```rust
use cq_client::Client;
use cq_e2e_tests::{restart_kept, start_server, stop_keeping_dir};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn create_view_then_subscribe_is_live() {
    let topic = TopicSpec::new("/positions", "position_id")
        .with_inline_columns([("position_id", "string"), ("sector", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/positions", json!({"position_id":"p1","sector":"TECH"}))
        .await
        .unwrap();
    client
        .publish("/positions", json!({"position_id":"p2","sector":"TECH"}))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;

    let resp = reqwest::Client::new()
        .post(format!("{}/admin/views", server.admin_url()))
        .json(&json!({
            "name": "/v_sector_counts",
            "source": "/positions",
            "sql": "SELECT sector, COUNT(*) AS n FROM t GROUP BY sector"
        }))
        .send()
        .await
        .expect("create-view POST");
    assert_eq!(resp.status().as_u16(), 201, "expected 201 Created");

    tokio::time::sleep(Duration::from_millis(150)).await;
    let rows = client
        .sow_sql("/v_sector_counts", "SELECT sector, n FROM t")
        .await
        .expect("view sow");
    let tech = rows
        .iter()
        .find(|r| r.get("sector").and_then(|v| v.as_str()) == Some("TECH"))
        .expect("TECH group");
    assert_eq!(tech.get("n").unwrap().as_i64().unwrap(), 2);

    // Live re-aggregation: a new source row bumps the count.
    client
        .publish("/positions", json!({"position_id":"p3","sector":"TECH"}))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    let rows = client
        .sow_sql("/v_sector_counts", "SELECT sector, n FROM t")
        .await
        .expect("view sow 2");
    let tech = rows
        .iter()
        .find(|r| r.get("sector").and_then(|v| v.as_str()) == Some("TECH"))
        .expect("TECH group 2");
    assert_eq!(tech.get("n").unwrap().as_i64().unwrap(), 3);
}

#[tokio::test]
async fn persisted_view_recreated_after_restart() {
    let topic = TopicSpec::new("/positions", "position_id")
        .with_inline_columns([("position_id", "string"), ("sector", "string")]);
    let server = start_server(vec![topic]).await;

    let resp = reqwest::Client::new()
        .post(format!("{}/admin/views", server.admin_url()))
        .json(&json!({
            "name": "/v_persist",
            "source": "/positions",
            "sql": "SELECT sector, COUNT(*) AS n FROM t GROUP BY sector"
        }))
        .send()
        .await
        .expect("create-view POST");
    assert!(resp.status().is_success());

    let rt = server.config_dir.join("runtime_views.toml");
    assert!(rt.exists(), "runtime_views.toml should be written on create");

    let kept = stop_keeping_dir(server).await;
    let server = restart_kept(kept).await;

    let body: Vec<Value> = reqwest::get(format!("{}/admin/catalog", server.admin_url()))
        .await
        .expect("catalog GET")
        .json()
        .await
        .expect("catalog json");
    let view = body.iter().find(|e| e["name"] == "/v_persist");
    assert!(view.is_some(), "view should be recreated after restart");
    assert_eq!(view.unwrap()["kind"], "view");
}

#[tokio::test]
async fn create_view_invalid_sql_is_rejected_and_not_persisted() {
    let topic = TopicSpec::new("/positions", "position_id")
        .with_inline_columns([("position_id", "string"), ("sector", "string")]);
    let server = start_server(vec![topic]).await;

    let resp = reqwest::Client::new()
        .post(format!("{}/admin/views", server.admin_url()))
        .json(&json!({
            "name": "/v_bad",
            "source": "/positions",
            "sql": "SELECT nonexistent_col, COUNT(*) AS n FROM t GROUP BY nonexistent_col"
        }))
        .send()
        .await
        .expect("create-view POST");
    assert!(
        !resp.status().is_success(),
        "invalid SQL must be rejected, got {}",
        resp.status()
    );

    let rt = server.config_dir.join("runtime_views.toml");
    let empty = !rt.exists()
        || std::fs::read_to_string(&rt).unwrap().trim().is_empty();
    assert!(empty, "rejected view must not be persisted");
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p cq-e2e-tests --test admin_views_runtime create_view 2>&1 | tail -30`
Expected: FAIL — POST `/admin/views` currently has no `post` handler, so it returns 405/404 (not 201).

- [ ] **Step 3: Add the handler + route**

In `crates/cq-server/src/admin.rs`, change the `/admin/views` route (line 92) from:

```rust
        .route("/admin/views", get(get_views))
```
to:
```rust
        .route("/admin/views", get(get_views).post(create_view))
```

Add the request type + handler after `get_views` (after line 463):

```rust
/// Body for `POST /admin/views`. `initial_capacity` / `tap_capacity`
/// are optional; sensible defaults mirror the config defaults.
#[derive(serde::Deserialize)]
struct CreateViewRequest {
    name: String,
    source: String,
    sql: String,
    initial_capacity: Option<usize>,
    tap_capacity: Option<usize>,
}

/// `POST /admin/views` — create a materialized view at runtime. Stands
/// the view up live via `init_view` (which validates the SQL, the
/// source topic, and the name), then persists it to the runtime-views
/// file so it is recreated on restart. v1 does NOT support teardown:
/// the runner/evaluator handle is detached.
async fn create_view(
    State(s): State<AdminState>,
    Json(req): Json<CreateViewRequest>,
) -> impl IntoResponse {
    if req.name.trim().is_empty()
        || req.source.trim().is_empty()
        || req.sql.trim().is_empty()
    {
        return (
            StatusCode::BAD_REQUEST,
            "name, source, and sql are required",
        )
            .into_response();
    }

    let entry = crate::config::ViewEntry {
        name: req.name.clone(),
        source: req.source.clone(),
        sql: req.sql.clone(),
        initial_capacity: req.initial_capacity.unwrap_or(10_000),
        tap_capacity: req.tap_capacity.unwrap_or(1024),
    };

    match crate::init_view(&entry, &s.topics, s.registry.clone()) {
        Ok(_handle) => {
            // v1: no teardown — detach the runner/evaluator handle.
            let canonical = cq_core::topic::canonicalize_topic(&entry.name);
            s.view_names.insert(canonical);
            if let Err(e) = crate::config::persist_runtime_view(&s.runtime_views_path, &entry) {
                // Live but not persisted — surface so the operator knows
                // it won't survive restart.
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("view created but persistence failed: {e}"),
                )
                    .into_response();
            }
            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "name": entry.name,
                    "source": entry.source,
                    "sql": entry.sql,
                    "initial_capacity": entry.initial_capacity,
                    "tap_capacity": entry.tap_capacity,
                })),
            )
                .into_response()
        }
        Err(e) => {
            // `init_view` returns "view name `…` collides…" on a name
            // clash; everything else (bad SQL, missing source) is a
            // client error.
            let code = if e.contains("collides") {
                StatusCode::CONFLICT
            } else {
                StatusCode::BAD_REQUEST
            };
            (code, format!("create view failed: {e}")).into_response()
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p cq-e2e-tests --test admin_views_runtime 2>&1 | tail -30`
Expected: PASS (all 4 tests in the file).

- [ ] **Step 5: Commit**

```bash
git add crates/cq-server/src/admin.rs crates/cq-e2e-tests/tests/admin_views_runtime.rs
git commit -m "feat(admin): POST /admin/views — runtime create + persist + recreate"
```

---

## Task 6: Full build + regression check

**Files:** none (verification only).

- [ ] **Step 1: Build the workspace**

Run: `cargo build --workspace 2>&1 | tail -20`
Expected: builds clean.

- [ ] **Step 2: Run the new e2e file + the existing admin tests**

Run: `cargo test -p cq-e2e-tests --test admin_views_runtime --test admin_endpoints --test admin_add_column 2>&1 | tail -30`
Expected: all PASS — the new endpoints don't regress existing admin behavior.

- [ ] **Step 3: Confirm config unit tests still pass**

Run: `cargo test -p cq-server 2>&1 | tail -20`
Expected: PASS (includes `runtime_views_tests`).

- [ ] **Step 4: Commit (if any incidental fixes were needed)**

```bash
git add -A
git commit -m "test: verify server-foundation views build + regressions green" || echo "nothing to commit"
```

---

## Self-Review (completed by author)

**Spec coverage** (against the design doc, Sub-project 1 section):
- `GET /admin/catalog` with `{name, kind, columns:[{name,type}]}` → Task 4. ✅
- `POST /admin/views` reusing `init_view`, 400/409 on error → Task 5. ✅
- Persist to dedicated `runtime_views.toml`, atomic write → Task 1 (`persist_runtime_view`), wired in Task 5. ✅
- Boot merge of `runtime_views.toml` before the init loop → Task 2. ✅
- `ViewEntry` gains `Serialize` → Task 1. ✅
- `AdminState` gains runtime-views path + live view-name set → Task 3. ✅
- Configurable runtime-views path (`runtime_views_path`, default `<config_dir>/runtime_views.toml`) → Task 2. ✅
- No live teardown (deferred) → handler detaches the handle (Task 5). ✅
- e2e: create→live, restart→recreated, catalog shape, invalid-SQL rejection → Tasks 4 & 5. ✅

**Placeholder scan:** No TBD/TODO/"handle errors" — every code step is complete. ✅

**Type consistency:** `init_view(cfg: &ViewEntry, topics: &Arc<DashMap<String, SharedTopic>>, registry: SessionRegistry) -> Result<JoinHandle<()>, String>` used consistently; `AdminState.view_names: Arc<DashSet<String>>` and `runtime_views_path: Arc<PathBuf>` referenced identically in Tasks 3/4/5; `load_runtime_views`/`persist_runtime_view`/`RuntimeViewsFile` names consistent across Tasks 1/2/5; `ViewEntry` fields (`name, source, sql, initial_capacity, tap_capacity`) match the struct at config.rs:626. ✅
