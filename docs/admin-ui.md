# cqserver admin UI — deploy + dev

The admin UI is a Vite + React 19 SPA living under
[`clients/admin-ui/`](../clients/admin-ui). In production it is served
**by the cqserver process itself** under `/ui/*`, sharing the admin
HTTP port (default `:8085`) with the existing JSON endpoints and
Prometheus `/metrics`.

## Dev loop

```sh
cd clients/admin-ui
npm install
npm run dev
# open http://localhost:5174
```

In dev mode the Vite server on `:5174` proxies `/admin-api/*` to
`http://127.0.0.1:8085`, so a local cqserver running with
`./start-demo.sh` (or any other dev launcher) is automatically
reachable. To point at a remote cqserver instead:

```sh
VITE_ADMIN_URL=http://other-host:8085 npm run dev
```

The dev build keeps the source maps + HMR enabled. Type-check
without serving:

```sh
npx tsc -b
```

## Production: serve from cqserver

The admin server in `cq-server` mounts a `ServeDir` under `/ui` at
startup. The bundle is resolved (in order) from:

1. `$CQSERVER_ADMIN_UI_DIR` — explicit override
2. `./clients/admin-ui/dist` — relative to the cqserver CWD
   (the standard dev layout, and what `start-demo.sh` produces)

If neither exists at startup, the server logs an "Admin UI dist not
found" notice and skips mounting `/ui`. JSON admin endpoints are
unaffected.

### Build + deploy

```sh
# 1. Build the SPA
cd clients/admin-ui
npm install
npm run build           # writes clients/admin-ui/dist/

# 2. Build the server (any time)
cd ../..
cargo build --release -p cq-server

# 3. Start the server from the repo root so the default `dist` path
#    resolves correctly
./target/release/cqserver --config config/cqserver.toml

# Open http://127.0.0.1:8085/ui
```

For a containerized deploy, copy both artifacts into the image and
either (a) keep the same `clients/admin-ui/dist` layout relative to
the binary's CWD, or (b) set `CQSERVER_ADMIN_UI_DIR=/srv/admin-ui` to
point at wherever the operator placed the bundle.

The admin UI's API base is auto-detected at runtime: when the SPA is
served from `/ui/*` it talks to the same origin's root (`/stats`,
`/topics`, etc.), with no proxy involved.

## Pages

| Route        | What it shows |
|---|---|
| `/`             | **Overview** — RSS, subscriptions, topics, publish rate, snapshot cache, replication topology, hottest topics table. 2-second poll. |
| `/topics`       | **Topics** — AG-Grid of every topic with row counts, capacity, key fields, sequence, schema state. |
| `/subscriptions`| **Subscriptions** — Live wire view: every active subscription, queue fill, drop count, slow-consumer indicator + per-row drop button. |
| `/views`        | **Views** — Split list + detail. Detail shows source linkage + materialized row count + the view's SQL body. |
| `/queues`       | **Queues** — One card per queue with buffered / consumers / sequence. |
| `/replication`  | **Replication** — Topology card + per-topic shipped / applied / acked / lag from `cq_repl_*` metrics. |
| `/metrics`      | **Metrics** — Prometheus series browser. Pin series to a sparkline grid that persists across reloads. |
| `/explain`      | **Explain** — Topic + SQL → `POST /admin/explain` → rows / bytes / confidence / used indexes / assumptions. |
| `/config`       | **Config** — Line-numbered, syntax-highlighted view of the running `cqserver.toml` with find-in-file + copy-to-clipboard. |

Every screen polls live; the connection-status pill in the header
shows green when `/healthz` is reachable and red when it isn't.

## Stack

- Vite 7 + React 19 + TypeScript
- Tailwind v4 (CSS variables shared with the design system at
  `/Users/develop/projects/design-system/react-app`)
- shadcn primitives (Radix-based; only the subset we actually use:
  button, card, badge, separator, scroll-area, tooltip)
- AG-Grid Community 35 for the Topics + Subscriptions tables
- TanStack Query 5 for polling + caching
- lucide-react for icons
- No state-management library; queries + per-page `useState` suffice

Total production bundle (post-Vite minify): ~310 KB gzipped JS,
~10 KB gzipped CSS.

## Keyboard shortcuts (Windows-style)

Bindings use `Ctrl` and `Alt` exclusively — no `⌘` — so the same
shortcut sheet works across Windows, Linux, and (with Ctrl, not Cmd)
macOS.

| Combo | Action |
|---|---|
| **Ctrl+K** | Open command palette (search across routes, topics, actions) |
| **Ctrl+/** | Open this cheat sheet from anywhere |
| **F5** | Re-poll the current page's data (no full page reload) |
| **Ctrl+F** | Focus the page's filter input (Topics, Subscriptions, Metrics, Config) |
| **Esc** | Close any open modal / palette / cheat sheet |
| **Alt+1** | Jump to Overview |
| **Alt+2** | Jump to Subscriptions |
| **Alt+3** | Jump to Replication |
| **Alt+4** | Jump to Topics |
| **Alt+5** | Jump to Views |
| **Alt+6** | Jump to Queues |
| **Alt+7** | Jump to Metrics |
| **Alt+8** | Jump to Explain |
| **Alt+9** | Jump to Config |
| ↑ / ↓ | Navigate within the command palette |
| Enter | Run the highlighted palette item |

Sidebar items show their `Alt+N` hint on hover. The keyboard icon in
the header opens the same cheat sheet (handy for operators who haven't
discovered the Ctrl+/ trigger yet).

### Implementation notes

- The keyboard layer lives in
  [`src/lib/keyboard.tsx`](../clients/admin-ui/src/lib/keyboard.tsx) —
  a single document-level keydown handler dispatches to a registry of
  `useShortcut`-registered callbacks.
- Shortcuts do **not** fire when an INPUT / TEXTAREA / SELECT / content-
  editable element is focused — operators can type filters without
  accidentally triggering F5 or Alt+1. Exceptions: `Escape` and
  `Ctrl+K` always fire (the latter is the escape hatch when something
  else has focus).
- Both modals auto-close on route change so a navigation triggered
  from the palette doesn't leave the overlay lingering.

## Aesthetic direction

Operator console — Bloomberg × Linear. Decisions baked in:

- **Dark mode default**, toggleable via the header. Operators run dark.
- **JetBrains Mono** for every numeric value (counts, IDs, sequences,
  bytes) so columns of numbers align visually. **Inter** for UI chrome.
- **Hairline borders + flat surfaces.** No drop shadows.
  `--card` is a single hairline-bordered panel; depth comes from
  content density, not from synthetic elevation.
- **Color used semantically:** `--primary` (blue) for actions, `--ok`
  (green) for healthy, `--warn` (amber) for slow / approaching limits,
  `--err` (red) for failures.
- **Motion** is reserved for continuously-flowing sparklines and
  brief value-change pulses on numeric cells. No entrance animations.

## What's deliberately not in here

- **Editing config / users.** Read-only. Edit on disk, restart, refresh.
- **Cluster-wide views.** Each cqserver instance serves its own admin
  UI; for multi-instance fleets put a thin reverse-proxy / sidecar in
  front, or just open each instance's UI in a separate tab.
- **PromQL.** Use Grafana — it owns that job. The Metrics screen here
  is for "what series exist, what are their current values" not for
  ad-hoc query authoring.
- **Auth on the admin port.** Today the admin port has no auth. Bind
  it to localhost (`admin_addr = "127.0.0.1:8085"` in `cqserver.toml`)
  or front it with a reverse-proxy that enforces auth.

## Worklog

The full session-by-session build history is in
[`ADMIN_UI_WORKLOG.md`](../ADMIN_UI_WORKLOG.md). U1 → U7 plus the
Windows-style hotkey + code-splitting follow-up are all shipped.
