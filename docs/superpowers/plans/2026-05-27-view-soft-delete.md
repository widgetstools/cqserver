# View Soft-Delete — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an operator delete a materialized view without a server restart — `DELETE /admin/views/{name}` stops the view's runner, removes it from the live topic registry + catalog + persisted runtime-views file, and surfaces a delete button in the admin UI.

**Architecture (soft delete):** Add tap-id tracking to `cq-core` so a source's view-tap `Sender` can be dropped by id (`unregister_view_tap`) — dropping it disconnects the runner's receiver so the **runner thread exits**. The view's **evaluator** thread is deliberately NOT torn down (it holds the topic `Arc` and blocks on `recv`; stopping it would require a core hot-path change we're avoiding). After delete, the orphaned evaluator sits idle (no more mutations reach it, no subscribers can find the topic) until the next restart reclaims it. The view is removed from the `topics` map, `view_names`, the `view_teardown` registry, and `runtime_views.toml`, so it disappears from the catalog and does not return on restart.

**Tech Stack:** Rust (cq-core taps, cq-server admin), React/TS (admin-ui). e2e via cq-e2e-tests.

This addresses the deferred "view delete/teardown" follow-up (soft variant, per decision). Builds on Sub-project 1 (`init_view`, `runtime_views.toml`, `/admin/views`, `view_names`) and the hardening batch (typed `InitViewError`).

**Known limitation (by design):** one idle evaluator thread per deleted view lingers until restart. Documented in the DELETE handler.

---

## File Structure

- **Modify** `crates/cq-core/src/topic.rs` — `view_taps` carries ids; `register_view_tap` returns `(u64, Receiver)`; add `unregister_view_tap(id)`; `fanout_view_tap` iterates tuples.
- **Modify** `crates/cq-server/src/main.rs` — destructure the new `register_view_tap` return in `init_view`; change `init_view` to return runner/teardown info; store teardown for boot views; add `view_teardown` to `AdminState` construction.
- **Modify** `crates/cq-server/src/admin.rs` — `AdminState.view_teardown`; `DELETE /admin/views/:name` handler + route.
- **Modify** `crates/cq-server/src/config.rs` — `remove_runtime_view(path, name)`.
- **Create** `crates/cq-e2e-tests/tests/admin_view_delete.rs` — e2e.
- **Modify** `clients/admin-ui/src/lib/admin.ts` — `deleteView(name)`.
- **Modify** `clients/admin-ui/src/pages/ViewsPage.tsx` — delete button + mutation.

---

## Task 1: Tap-id tracking + `unregister_view_tap` (cq-core)

**Files:**
- Modify: `crates/cq-core/src/topic.rs` — the `view_taps` field (~line 176), its init (~247), `register_view_tap` (~528-531), `fanout_view_tap` (~538-548).
- Modify: `crates/cq-server/src/main.rs` — the two `register_view_tap` call sites in `init_view` (the `left_tap` and `right_tap` bindings).
- Test: inline `#[cfg(test)]` in `crates/cq-core/src/topic.rs`.

- [ ] **Step 1: Write the failing test**

Add to the existing test module in `crates/cq-core/src/topic.rs` (or a new `#[cfg(test)] mod view_tap_tests` at the end). It exercises register → fanout reaches the tap → unregister → fanout no longer reaches it and the receiver disconnects:

```rust
#[cfg(test)]
mod view_tap_tests {
    use super::*;
    use crate::schema::{ColumnType, Schema};

    fn mk_topic() -> Topic {
        let schema = Schema::from_strs(&["k", "v"], &[ColumnType::String, ColumnType::Long]);
        Topic::new(
            TopicConfig {
                name: "/tap-test".into(),
                key_fields: vec!["k".into()],
                ..Default::default()
            },
            schema,
        )
    }

    #[test]
    fn unregister_view_tap_drops_sender_and_disconnects_receiver() {
        let topic = mk_topic();
        let (id, rx) = topic.register_view_tap(16);
        // Publish a row → the tap should receive a mutation event.
        topic.upsert_json(&serde_json::json!({ "k": "a", "v": 1 })).unwrap();
        assert!(rx.recv_timeout(std::time::Duration::from_millis(200)).is_ok());

        // Unregister → the sender is dropped → the receiver disconnects.
        topic.unregister_view_tap(id);
        // A further publish must NOT deliver (sender gone); the channel is
        // empty + disconnected, so recv returns Err.
        topic.upsert_json(&serde_json::json!({ "k": "b", "v": 2 })).unwrap();
        assert!(rx.recv_timeout(std::time::Duration::from_millis(200)).is_err());
    }
}
```

Note: confirm the exact constructor/`TopicConfig` fields and the publish method name by reading the existing tests in `topic.rs` (e.g. how other tests build a `Topic` and insert a row — match that helper; the snippet above uses plausible names `Topic::new`/`TopicConfig`/`upsert_json` that MUST be aligned to the real API before running). If the real insert method differs (e.g. `apply_upsert`, `publish_json`), use it.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p cq-core view_tap_tests 2>&1 | tail -15`
Expected: FAIL — `register_view_tap` returns `Receiver` (not a tuple) and `unregister_view_tap` doesn't exist.

- [ ] **Step 3: Implement tap-id tracking**

In `crates/cq-core/src/topic.rs`:

- Change the field (~line 176) from:
  ```rust
  view_taps: Mutex<Vec<Sender<MutationEvent>>>,
  ```
  to:
  ```rust
  view_taps: Mutex<Vec<(u64, Sender<MutationEvent>)>>,
  next_view_tap_id: std::sync::atomic::AtomicU64,
  ```
- In the constructor (~line 247) where `view_taps: Mutex::new(Vec::new()),` is set, add alongside:
  ```rust
  next_view_tap_id: std::sync::atomic::AtomicU64::new(0),
  ```
- Replace `register_view_tap` (~528-531):
  ```rust
  pub fn register_view_tap(&self, cap: usize) -> (u64, Receiver<MutationEvent>) {
      let (tx, rx) = crossbeam_channel::bounded(cap);
      let id = self
          .next_view_tap_id
          .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
      self.view_taps.lock().push((id, tx));
      (id, rx)
  }

  /// Drop the view-tap `Sender` registered under `id`. The matching
  /// runner's `Receiver` then disconnects and its thread exits. No-op
  /// if the id isn't present.
  pub fn unregister_view_tap(&self, id: u64) {
      self.view_taps.lock().retain(|(tid, _)| *tid != id);
  }
  ```
  (Confirm the channel constructor used by the original — it's `crossbeam_channel::bounded(cap)`; match the original's exact form.)
- Update `fanout_view_tap` (~538-548): the `retain` closure now destructures the tuple:
  ```rust
  taps.retain(|(_, tx)| match tx.try_send(event.clone()) {
      // ...keep the existing Ok / Full / Disconnected arms unchanged...
  });
  ```

In `crates/cq-server/src/main.rs` `init_view`, update the two call sites:
- `let left_tap = source.register_view_tap(cfg.tap_capacity);` → `let (left_tap_id, left_tap) = source.register_view_tap(cfg.tap_capacity);`
- `let right_tap = right_topic_opt.as_ref().map(|r| r.register_view_tap(cfg.tap_capacity));` → keep mapping but destructure: this yields `Option<(u64, Receiver)>`. Change to:
  ```rust
  let right_tap = right_topic_opt
      .as_ref()
      .map(|r| r.register_view_tap(cfg.tap_capacity));
  let (right_tap_id, right_tap_rx) = match right_tap {
      Some((id, rx)) => (Some(id), Some(rx)),
      None => (None, None),
  };
  ```
  Then wherever `right_tap` (the Receiver) was passed to `spawn_view_runner_joined`, pass `right_tap_rx` instead. (`left_tap_id`/`right_tap_id` are used by Task 2's teardown capture; for THIS task just make it compile — they may be unused-warned until Task 2 wires them; add `let _ = (left_tap_id, right_tap_id);` ONLY if needed to avoid an error, but a warning is acceptable.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p cq-core view_tap_tests 2>&1 | tail -15`
Expected: PASS. Also `cargo build -p cq-server 2>&1 | tail -5` clean.

- [ ] **Step 5: Commit**

```bash
git add crates/cq-core/src/topic.rs crates/cq-server/src/main.rs
git commit -m "feat(cq-core): id-tracked view taps + unregister_view_tap

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Teardown registry + `DELETE /admin/views/{name}` (cq-server)

**Files:**
- Modify: `crates/cq-server/src/config.rs` — add `remove_runtime_view`.
- Modify: `crates/cq-server/src/main.rs` — a `ViewTeardown` struct; `init_view` returns it; boot loop stores it; `AdminState` construction passes the registry.
- Modify: `crates/cq-server/src/admin.rs` — `AdminState.view_teardown` field; `delete_view` handler + route.
- Test: `crates/cq-e2e-tests/tests/admin_view_delete.rs` (new).

- [ ] **Step 1: `remove_runtime_view` in config.rs**

Add next to `persist_runtime_view`:

```rust
/// Remove a view (by canonical or raw name match) from the runtime-views
/// file. Returns true if an entry was removed. No-op (returns false) if
/// the file is absent or the name isn't present.
pub fn remove_runtime_view(
    path: &std::path::Path,
    name: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let mut views = load_runtime_views(path)?;
    let before = views.len();
    let want = crate::config::canonical_name(name);
    views.retain(|v| crate::config::canonical_name(&v.name) != want);
    if views.len() == before {
        return Ok(false);
    }
    let file = RuntimeViewsFile { views };
    let toml_text = toml::to_string_pretty(&file)?;
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, toml_text)?;
    std::fs::rename(&tmp, path)?;
    Ok(true)
}

/// Canonicalize a topic/view name the same way the topic registry does
/// (slash-prefixed), so file matching agrees with the live map keys.
fn canonical_name(name: &str) -> String {
    cq_core::topic::canonicalize_topic(name)
}
```
(If `cq_core::topic::canonicalize_topic` isn't already imported/visible in config.rs, reference it fully-qualified as shown. If a `canonical_name` helper already exists, reuse it instead of redefining.)

- [ ] **Step 2: `ViewTeardown` + `init_view` returns it (main.rs)**

Add near `InitViewError`:

```rust
/// What `DELETE /admin/views/{name}` needs to stop a view's runner:
/// drop the source view-tap(s) by id. (The evaluator is not torn down —
/// soft delete leaves it idle until restart.)
#[derive(Clone)]
pub(crate) struct ViewTeardown {
    pub source: SharedTopic,
    pub left_tap_id: u64,
    pub right: Option<(SharedTopic, u64)>,
}
```

Change `init_view`'s return type from `Result<std::thread::JoinHandle<()>, InitViewError>` to:
```rust
Result<(std::thread::JoinHandle<()>, ViewTeardown), InitViewError>
```
Build the `ViewTeardown` from the tap ids captured in Task 1 (`left_tap_id`, and `right_tap_id` + the `right_topic_opt`), and return it alongside the evaluator handle. Concretely, where the function currently ends with `Ok(evaluator_handle)`, change to:
```rust
let teardown = ViewTeardown {
    source: source.clone(),
    left_tap_id,
    right: match (right_topic_opt.clone(), right_tap_id) {
        (Some(rt), Some(rid)) => Some((rt, rid)),
        _ => None,
    },
};
Ok((evaluator_handle, teardown))
```
(Ensure `source` is still in scope / cloneable at the end — it's the `SharedTopic` resolved earlier. If it was moved into `View::new`, clone it before the move and keep a `source_for_teardown` binding. Adjust as the borrow checker requires.)

Update the boot loop (~line 262-280) that calls `init_view`: it currently does `Ok(handle) => { evaluator_handles.push(handle); ... }`. Change to destructure and stash teardown into a local map that will seed `AdminState.view_teardown`:
```rust
let view_teardown: Arc<DashMap<String, ViewTeardown>> = Arc::new(DashMap::new());
// ... in the loop:
Ok((handle, teardown)) => {
    evaluator_handles.push(handle);
    view_teardown.insert(
        cq_core::topic::canonicalize_topic(&view_cfg.name),
        teardown,
    );
    info!(view = %view_cfg.name, source = %view_cfg.source, "Materialized view ready");
}
```
(Declare `view_teardown` BEFORE the loop. It must be in scope at the `AdminState { ... }` construction.)

In the `AdminState { ... }` literal, add:
```rust
        view_teardown: view_teardown.clone(),
```

- [ ] **Step 3: AdminState field + `delete_view` handler (admin.rs)**

In `pub struct AdminState`, add:
```rust
    /// Per-view teardown info (source + tap ids) so DELETE /admin/views
    /// can stop a view's runner. Soft delete: the evaluator lingers.
    pub view_teardown: Arc<dashmap::DashMap<String, crate::ViewTeardown>>,
```

In `create_view`'s success path (after `s.view_names.insert(canonical.clone())`), ALSO record teardown — but `create_view` calls `crate::init_view` which now returns `(handle, teardown)`. Update `create_view`'s `Ok` arm to destructure `Ok((_handle, teardown))` and `s.view_teardown.insert(canonical.clone(), teardown);` before persisting. (Adjust the existing `Ok(_handle) =>` arm accordingly; `canonical` is already computed there.)

Add the route in `start_admin_server` (next to the `/admin/views` route):
```rust
        .route("/admin/views/:name", delete(delete_view))
```
(`delete` is already imported from `axum::routing` — it's used by `/subscriptions/:sub_id`.)

Add the handler:
```rust
/// `DELETE /admin/views/{name}` — soft delete. Stops the view's runner
/// (drops the source view-tap so the runner's receiver disconnects),
/// removes the view from the live topic registry + catalog set + the
/// persisted runtime-views file, so it disappears now and does not
/// return on restart. The view's evaluator thread is NOT torn down (it
/// holds the topic Arc and blocks on recv); it sits idle until the next
/// restart. Returns 404 if the name isn't a known view.
async fn delete_view(
    State(s): State<AdminState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let canonical = cq_core::topic::canonicalize_topic(&name);
    let Some((_, teardown)) = s.view_teardown.remove(&canonical) else {
        return (StatusCode::NOT_FOUND, format!("no such view: {canonical}")).into_response();
    };
    // Stop the runner: dropping the source tap sender disconnects the
    // runner's receiver (and, for joined views, the fan-in thread).
    teardown.source.unregister_view_tap(teardown.left_tap_id);
    if let Some((right, rid)) = teardown.right {
        right.unregister_view_tap(rid);
    }
    // Remove from the live registry + catalog tagging.
    s.topics.remove(&canonical);
    s.view_names.remove(&canonical);
    // De-persist so it doesn't return on restart.
    if let Err(e) = crate::config::remove_runtime_view(&s.runtime_views_path, &canonical) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("view stopped but de-persist failed: {e}"),
        )
            .into_response();
    }
    (StatusCode::OK, Json(serde_json::json!({ "deleted": canonical }))).into_response()
}
```
(`Path` is already imported; `delete` from axum routing is imported. `crate::ViewTeardown` is the struct from main.rs.)

- [ ] **Step 4: Write the e2e test**

Create `crates/cq-e2e-tests/tests/admin_view_delete.rs`:

```rust
//! Soft-delete e2e — DELETE /admin/views/{name} stops + de-registers
//! + de-persists a runtime-created view; it does not return on restart.

use cq_e2e_tests::{restart_kept, start_server, stop_keeping_dir, TopicSpec};
use serde_json::{json, Value};
use std::time::Duration;

async fn create(server_admin_url: &str, name: &str) {
    let resp = reqwest::Client::new()
        .post(format!("{server_admin_url}/admin/views"))
        .json(&json!({
            "name": name,
            "source": "/positions",
            "sql": "SELECT sector, COUNT(*) AS n FROM t GROUP BY sector"
        }))
        .send()
        .await
        .expect("create");
    assert_eq!(resp.status().as_u16(), 201, "create should 201");
}

fn topic() -> TopicSpec {
    TopicSpec::new("/positions", "position_id")
        .with_inline_columns([("position_id", "string"), ("sector", "string")])
}

#[tokio::test]
async fn delete_removes_view_from_catalog_and_persistence() {
    let server = start_server(vec![topic()]).await;
    create(&server.admin_url(), "/v_del").await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Present in catalog before delete.
    let before: Vec<Value> = reqwest::get(format!("{}/admin/catalog", server.admin_url()))
        .await.unwrap().json().await.unwrap();
    assert!(before.iter().any(|e| e["name"] == "/v_del"), "view present pre-delete");

    // DELETE → 200.
    let del = reqwest::Client::new()
        .delete(format!("{}/admin/views/{}", server.admin_url(), "%2Fv_del"))
        .send().await.expect("delete");
    assert_eq!(del.status().as_u16(), 200, "delete should 200");

    // Gone from catalog + runtime_views.toml.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let after: Vec<Value> = reqwest::get(format!("{}/admin/catalog", server.admin_url()))
        .await.unwrap().json().await.unwrap();
    assert!(!after.iter().any(|e| e["name"] == "/v_del"), "view gone post-delete");
    let rt = server.config_dir.join("runtime_views.toml");
    let persisted = if rt.exists() { std::fs::read_to_string(&rt).unwrap() } else { String::new() };
    assert!(!persisted.contains("/v_del"), "view de-persisted");

    // Restart → still gone.
    let kept = stop_keeping_dir(server).await;
    let server = restart_kept(kept).await;
    let post: Vec<Value> = reqwest::get(format!("{}/admin/catalog", server.admin_url()))
        .await.unwrap().json().await.unwrap();
    assert!(!post.iter().any(|e| e["name"] == "/v_del"), "view not resurrected on restart");
}

#[tokio::test]
async fn delete_unknown_view_returns_404() {
    let server = start_server(vec![topic()]).await;
    let del = reqwest::Client::new()
        .delete(format!("{}/admin/views/{}", server.admin_url(), "%2Fv_nope"))
        .send().await.expect("delete");
    assert_eq!(del.status().as_u16(), 404, "deleting a missing view should 404");
}
```

- [ ] **Step 5: Build + test**

```bash
cargo build --release -p cq-server 2>&1 | tail -3
cargo test -p cq-e2e-tests --test admin_view_delete --test admin_views_runtime 2>&1 | tail -20
```
Expected: builds clean; the 2 new delete tests + the 5 existing admin_views_runtime tests all pass (delete must not regress create/catalog/restart).

- [ ] **Step 6: Commit**

```bash
git add crates/cq-server/src/config.rs crates/cq-server/src/main.rs crates/cq-server/src/admin.rs crates/cq-e2e-tests/tests/admin_view_delete.rs
git commit -m "feat(admin): DELETE /admin/views/{name} soft-delete (stop runner + de-register + de-persist)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Delete button in the admin UI

**Files:**
- Modify: `clients/admin-ui/src/lib/admin.ts` — add `deleteView`.
- Modify: `clients/admin-ui/src/pages/ViewsPage.tsx` — delete button in the view detail panel + mutation.

- [ ] **Step 1: API method**

In `admin.ts`, add to the `adminApi` object (the `del` helper already exists):
```ts
  deleteView: (name: string) => del(`/admin/views/${encodeURIComponent(name)}`),
```

- [ ] **Step 2: Delete control on ViewsPage**

In `ViewsPage.tsx`:
- Imports: add `useMutation`/`useQueryClient` are already imported (from the create form); add `Trash2` to the lucide import.
- In the `ViewDetail` component, accept an `onDeleted` prop or inline a delete mutation. Simplest: lift a delete handler into `ViewsPage` and pass it down. Add inside `ViewsPage` (it already has `useQueryClient` via the form? No — that's in CreateViewForm). Add at the top of `ViewsPage`:
  ```ts
  const qc = useQueryClient();
  const del = useMutation({
    mutationFn: (name: string) => adminApi.deleteView(name),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['views'] });
      qc.invalidateQueries({ queryKey: ['catalog'] });
      setSelected(null);
    },
  });
  ```
- Pass `onDelete={() => del.mutate(view.name)}` and `deleting={del.isPending}` into `<ViewDetail ... />`, and in `ViewDetail`'s header add a destructive button:
  ```tsx
  <Button
    variant="destructive"
    size="sm"
    onClick={onDelete}
    disabled={deleting}
  >
    <Trash2 size={11} /> {deleting ? 'Deleting…' : 'Delete'}
  </Button>
  ```
  Extend `ViewDetail`'s props with `onDelete: () => void; deleting: boolean;`. Place the button in the `CardHeader` next to the title (make the header a flex row if it isn't).
- Optionally surface `del.isError` near the detail header: `{del.isError ? <span className="text-[11.5px] text-err">{(del.error as Error).message}</span> : null}`.

- [ ] **Step 3: Build**

Run: `cd clients/admin-ui && npm run build 2>&1 | tail -5`
Expected: `tsc -b` + vite build clean.

- [ ] **Step 4: Commit**

```bash
git add clients/admin-ui/src/lib/admin.ts clients/admin-ui/src/pages/ViewsPage.tsx
git commit -m "feat(admin-ui): delete button on the Views page

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Manual verification (user-run)

- [ ] Launch `./start-atlas-demo.sh` + `cd clients/admin-ui && npm run dev`.
- [ ] Admin UI → Views → create `/v_del_demo`; confirm it appears.
- [ ] Select it, click **Delete** → it vanishes from the list; `config/runtime_views.toml` no longer contains it.
- [ ] Restart cqserver → `/v_del_demo` does not reappear.
- [ ] (Optional) `curl -XDELETE localhost:8085/admin/views/%2Fv_nope` → 404.

No commit (verification only).

---

## Self-Review (completed by author)

**Spec coverage:**
- Stop the runner without restart → Task 1 (`unregister_view_tap`) + Task 2 (DELETE drops the tap). ✅
- Remove from catalog/registry → Task 2 (`topics.remove` + `view_names.remove`). ✅
- De-persist (no restart resurrection) → Task 1.config `remove_runtime_view` + Task 2 DELETE + e2e restart assertion. ✅
- UI affordance → Task 3 delete button. ✅
- Evaluator NOT torn down (soft) → documented in the DELETE handler; accepted limitation. ✅
- 404 on unknown view → Task 2 handler + e2e. ✅

**Placeholder scan:** No TBD/TODO. The two "confirm the real API name" notes (Task 1 Topic constructor/insert method; config `canonical_name` reuse) are explicit verification steps with concrete fallbacks, not vague gaps.

**Type/name consistency:** `register_view_tap` now returns `(u64, Receiver)` (Task 1) — consumed in `init_view` (Task 1) and the ids flow into `ViewTeardown` (Task 2); `ViewTeardown` defined in main.rs, referenced by `AdminState.view_teardown` (admin.rs) and the boot loop; `unregister_view_tap(id)` (Task 1) called by `delete_view` (Task 2); `remove_runtime_view` (Task 2.1) called by `delete_view`; `deleteView` (Task 3) hits the `DELETE /admin/views/:name` route (Task 2). ✅

**Scope:** Soft delete only — no evaluator hot-path change. cq-core change is confined to tap bookkeeping. Self-contained across the 3 layers with e2e. ✅
