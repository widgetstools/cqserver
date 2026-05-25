# Admin UI Worklog (Galvanometer-class operator console)

**Goal.** A first-class web operator console for cqserver — comparable
in scope to AMPS Galvanometer — that lets a human operator answer
the questions they actually ask during an incident: "Is the server
healthy? Which topics are hot? Who's subscribed and what are they
asking for? Is replication keeping up? Why is RSS climbing?"

The existing admin surface today (`/`, `/fi-demo`) is a pair of
hand-rolled HTML pages with iframes — fine for the demo, inadequate
for an operator. This worklog plans a replacement built as a real
SPA against the same `/admin/*` endpoints.

**Stack (deliberately chosen, not negotiable):**

- **Vite 7** + **React 19** + **TypeScript 5.9**
- **Tailwind CSS v4** (the same `@theme`-based setup the design
  system uses)
- **shadcn/ui** primitives (Radix-based; ~15 of the 40 are needed
  for this app)
- **AG-Grid Community 35** (Enterprise license can land later if
  tree-data / pivot becomes a need — Community is sufficient for
  every screen in this plan)
- **TanStack Query 5** for polling/caching admin responses
- **Recharts** for sparklines
- **lucide-react** for icons
- The existing **design system** at
  `/Users/develop/projects/design-system/react-app` provides the
  CSS-variable palette, fonts (Inter + JetBrains Mono), shadcn
  primitives, ThemeProvider, AppShell/Sidebar — we **consume**
  it, we don't reinvent it.

**Aesthetic direction:** Operator console — Bloomberg × Linear.

- Dark mode default (`<html class="dark">`); light mode toggleable.
- JetBrains Mono for all data (counts, IDs, sequences, RSS); Inter
  for chrome (labels, nav, headings).
- Strong vertical rhythm. Hairline (1 px) dividers, no fake depth
  shadows. Cards have a single-pixel border and `--card` background.
- Motion reserved for: continuously-flowing sparklines, value-change
  pulses (a 400 ms tint when a numeric updates), and grid-row
  highlight on selection. No "page entrance" animations.
- Color used semantically: blue (`--primary`) for actions, green
  (`--buy`) for healthy / increasing, amber for warnings, red
  (`--destructive`) for failures, neutral grays for everything else.
- Information density first, whitespace second. An operator at 3 AM
  during an incident should see all the relevant numbers without
  scrolling.

**Scope guard.** This worklog covers ONLY:

- Read-mostly operator views (dashboard, topics, subscriptions,
  views, queues, replication, metrics, query explain).
- Topic-level write actions that already exist on the admin API
  (rotate journal, shrink store, drop a subscription).
- Dark/light mode + responsive desktop layout.
- Real wire data — every screen consumes `/admin/*` endpoints,
  not mocks.

Out of scope:

- Writing data (publish / delete rows) — that's an SDK concern,
  not an admin concern.
- Bulk auth / user management — `auth.users` lives in TOML; this
  worklog displays it but doesn't mutate it.
- Configuration editing — read-only view of `cqserver.toml`.
- Mobile / phone layouts — operators run desktop.
- WebSocket-driven live subscription previews — interesting but
  S2b's reconnect work needs to land first.

---

## Existing pieces (verified before scoping)

What's already in the tree to consume:

- **Admin HTTP API** on `0.0.0.0:8085`:
  `/healthz`, `/stats`, `/topics`, `/subscriptions`,
  `/subscriptions/:sub_id` (DELETE),
  `/metrics` (Prometheus),
  `/admin/rotate-journal/:topic`,
  `/admin/shrink-store/:topic`, `/admin/shrink-store-all`,
  `/admin/replication`,
  `/admin/shard-for/:topic`.
- **Design system** at
  `/Users/develop/projects/design-system/react-app`:
  - `src/styles/globals.css` — full token palette, light + dark.
  - `src/components/ui/*` — 40 shadcn primitives.
  - `src/components/theme/ThemeProvider.tsx` — dark/light toggle
    that also syncs the AG-Grid theme via
    `documentElement.dataset.agThemeMode`.
  - `src/components/layout/{AppShell,Header,Sidebar}.tsx` — layout
    skeleton.
- The `clients/react-demo/` app — different audience (demo
  consumers, not operators) but shares the design language.

We **consume** the design system by copying its tokens + the subset
of primitives we need into `clients/admin-ui/src/`. We do NOT
reach across `/Users/develop/projects/design-system/...` at
runtime — that path is outside the repo and the admin UI must
build standalone.

---

## Sessions

### U1 — Scaffold + shell + admin API client

**Goal.** Make `cd clients/admin-ui && npm run dev` produce a
running app with the navigation chrome and a live connection to
a local cqserver admin endpoint. No real screens yet.

**Deliverables:**

- `clients/admin-ui/` new Vite + React 19 + TS project.
- Tailwind v4 + Tailwind Vite plugin, importing the design
  system's `globals.css` (copied locally).
- Inter + JetBrains Mono via fontsource.
- `ThemeProvider` copied + adapted (default dark).
- AG-Grid Community + the theme-mode sync.
- AppShell with:
  - Sidebar (logo / name / nav items: Overview, Topics, Views,
    Subscriptions, Queues, Replication, Metrics, Explain,
    Config).
  - Header (server URL, health pill, theme toggle, version).
  - Main content area (route outlet).
- `react-router-dom` v7 with the routes wired up (each is a
  placeholder Page component for now).
- `src/lib/admin.ts` — typed wrappers around every admin
  endpoint, using `fetch` + TanStack Query.
- `.env`-driven `VITE_ADMIN_URL` (default `http://127.0.0.1:8085`).
- A simple polling rule: `/healthz` + `/stats` every 2 s; topics /
  subs every 5 s. TanStack Query handles the cache.

**Definition of done:**
- `npm run dev` serves on `http://localhost:5174/`.
- Switching between dark / light works without page reload and
  also flips the AG-Grid theme.
- The header shows a green health pill when the configured
  admin URL responds to `/healthz`, red when it doesn't.

**Estimated effort:** ~1 day.

---

### U2 — Overview page

**Goal.** The "first screen you open during an incident" — answers
"is the server alive and healthy?" in 5 seconds.

**Layout:**

```
┌─────────────────────────────────────────────────────────────┐
│  RSS         Topics    Subscriptions   Active routes        │
│  ▁▂▅▆▅▃▂ ▁  47        2,143           3,402                 │
│  856 MB         ↑ +18 (5m)    ↑ +210 (5m)                   │
├─────────────────────────────────────────────────────────────┤
│  Replication                  │  Snapshot cache             │
│  ◉ Standalone                 │  Bytes  118 / 256 MB        │
│  no follower / leader         │  Hits   ▆▅▆▇▅▅▅ 94.2 %      │
│                               │  Comp.  14.3 % (zstd)       │
├─────────────────────────────────────────────────────────────┤
│  Hottest topics (1m)                                        │
│  ┌──────────────┬────────┬────────┬────────┬────────────┐   │
│  │ Topic        │ Rows   │ Pub/s  │ Subs   │ Mutations  │   │
│  │ /trades      │ 865938 │ 142    │   3    │ ▂▅▇▆▅▃     │   │
│  │ /positions   │ 40000  │  18    │   1    │ ▁▂▃▂▁▁     │   │
│  │ ...                                                   │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**Components:**
- `MetricCard` — large numeric, optional sparkline, optional delta
  vs. window start.
- `Sparkline` — Recharts; auto-scales; pure SVG, ~24 px tall.
- `HotTopicsTable` — AG-Grid Community, ~10 rows, embedded sparkline
  cell renderer.
- Replication panel — collapses to "Standalone" when role = standalone.

**Data:**
- `/stats` (every 2 s) for the headline numbers.
- `/topics` (every 5 s) for the table.
- `/metrics` (every 2 s) parsed for `cq_publish_total`,
  `cq_snapshot_cache_bytes`, replication lag etc. — derive
  per-second rates from successive samples.
- `/admin/replication` (every 2 s) for the replication panel.

**Definition of done:**
- All four headline numbers tick continuously.
- The sparkline animates smoothly (~60 fps).
- Click any topic row → navigates to that topic's detail page
  (which lands in U3).

**Estimated effort:** ~1-2 days.

---

### U3 — Topics page + topic detail

**Goal.** Drill from the topics list into a single topic's
schema, SOW browser, mutation stream, and admin actions.

**List view:**
- AG-Grid Community of `/topics` rows.
- Columns: name (clickable), rowCount, columnCount, keyFields,
  subscriptions, globalVersion, capacity, schemaDiscovered,
  persist, conflation_ms.
- Server-side actions per row (kebab menu): rotate journal,
  shrink store, copy schema JSON, view in detail.

**Detail view:**
- Header: topic name, key fields, schema-discovered toggle, persist
  badge, current sequence.
- Tabs: SOW Browser • Mutations • Schema • Subscriptions • Actions.
- SOW Browser tab:
  - AG-Grid with the topic's actual schema as columns.
  - WHERE filter input on top — issued via a server-side SOW
    request (one-shot, no continuous subscribe).
  - Result-row count + bytes shown above the grid.
- Mutations tab: live tail (the existing
  `cq-client::Subscription` pattern, but read-only and time-bound
  — auto-pauses after 30 s of inactivity).
- Schema tab: column list with type, indexed badge, nullable.
- Subscriptions tab: filtered slice of `/subscriptions` for this
  topic.
- Actions tab: rotate journal, shrink store, drop all subs (with
  confirmation modal).

**Definition of done:**
- Clicking a topic name in U2's list lands on the detail page.
- The SOW Browser successfully runs a WHERE query and returns
  rows.
- Mutations tab shows the live publisher's stream when the demo
  publisher is running.

**Estimated effort:** ~2-3 days.

---

### U4 — Subscriptions + Queues

**Goal.** Visibility into the live wire: who is connected, what
they asked for, are they keeping up.

**Subscriptions list:**
- AG-Grid of `/subscriptions`.
- Columns: sub_id, session, topic, filter / SQL, sequence,
  status, drop count, queue depth, connected since.
- Group-by session (collapsible).
- Per-row kebab: drop subscription (`DELETE /subscriptions/:id`).
- Slow-consumer indicator — red dot when drops > threshold.

**Queues view:**
- One card per queue topic.
- Depth, in-flight messages, leased messages, max lease age, DLQ
  link, expired messages count.
- Click a queue → live tail of messages (similar to topic
  mutations tab).

**Definition of done:**
- Drop-sub action removes the row within one poll cycle.
- Slow-consumer indicator turns red when a stress run drops
  > 100 msg/sec for that sub.

**Estimated effort:** ~1-2 days.

---

### U5 — Views + Replication + Config

**Views:**
- Like the Topics list but for `[[views]]`.
- Same detail tabs (no SOW Browser — view rows ARE the SOW).
- Plus a "Source" tab showing the SQL and source-topic link.

**Replication:**
- Per-topic table of shipped / acked / applied sequences.
- Lag = shipped - applied per topic (ms estimate from sequence
  rate).
- Connection status panel (peer URL, last connect, reconnect count).
- Filter / transform display (read-only).

**Config viewer:**
- Read-only render of the running server's `cqserver.toml`.
- Syntax-highlighted (Shiki).
- Inline doc links to the relevant TOML reference for each
  section.

**Definition of done:**
- Replication lag is computed correctly when a real shipper
  is running.
- Config viewer mirrors the live config (NOT the on-disk file —
  these can drift after env-var substitution).

**Estimated effort:** ~1-2 days.

---

### U6 — Metrics explorer + Query Explain

**Metrics explorer:**
- Pull `/metrics` (Prometheus text format), parse client-side.
- Browse by name, drill into label dimensions.
- Pin metrics → live sparkline grid.
- No PromQL — operators use Grafana for that; this is the
  in-process "what metric exists, what's its current value"
  console.

**Query Explain:**
- Form: topic + SQL text area.
- Calls `POST /admin/explain` (lands in QUERY_GUARDRAILS_WORKLOG
  G2).
- Displays the estimated cost: rows, bytes, fanout, indexes used,
  assumptions, confidence.
- Warns if the query would be rejected by current limits.
- "Try as subscribe" button — fires a real subscribe, returns
  the actual cost, and shows the estimate-vs-actual delta.

**Definition of done:**
- Both screens function when their dependencies are in place
  (`/metrics` is real today; `/admin/explain` lands with G2).
- Metrics explorer doesn't OOM the browser on a 10 K-series
  cqserver.

**Estimated effort:** ~2 days.

---

### U7 — Polish + production build

- Hot-swap pages without flickering (React Suspense at the route
  boundary).
- Keyboard navigation across sidebar (j/k, gg, etc., Linear-style).
- A focused "command palette" (⌘K) that lets an operator type
  `drop sub abc123` or `rotate /trades` and run the action
  without navigation.
- Production Vite build (`npm run build`) producing a static bundle
  that the cqserver admin HTTP server can serve under `/ui/*`
  (an alternative landing to today's iframes).
- Documentation: `docs/admin-ui.md` covering deploy + dev.

**Definition of done:**
- `cargo run -p cq-server` serves the admin UI at `/ui/`.
- The whole bundle is < 1.5 MB gzipped.

**Estimated effort:** ~1-2 days.

---

## Order of execution

U1 → U2 → U3 → U4 → U5 → U6 → U7. Strict serial because each
session lands a screen that the next builds on. U6 depends on
QUERY_GUARDRAILS G2 landing before the Explain tab is fully
functional (the screen scaffold lands earlier, behind a "G2
not available" empty state).

## Status

| # | Session | Status |
|---|---|---|
| U1 | Scaffold + shell + admin API client | ⏳ in progress (this session) |
| U2 | Overview page | ⏳ pending |
| U3 | Topics page + topic detail | ⏳ pending |
| U4 | Subscriptions + Queues | ⏳ pending |
| U5 | Views + Replication + Config | ⏳ pending |
| U6 | Metrics explorer + Query Explain | ⏳ pending |
| U7 | Polish + production build | ⏳ pending |

## Related worklogs

- `QUERY_GUARDRAILS_WORKLOG.md` — U6's Explain tab needs G2's
  `/admin/explain` endpoint.
- `REPLICA_READS_WORKLOG.md` — U5's Replication screen relies on
  the metrics exposed by the replication subsystem.
