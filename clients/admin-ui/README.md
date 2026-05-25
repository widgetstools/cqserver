# cqserver admin UI

Galvanometer-class operator console for cqserver. Vite + React 19 +
Tailwind v4 + shadcn primitives + AG-Grid Community.

## Quick start

```sh
cd clients/admin-ui
npm install
npm run dev
# open http://localhost:5174
```

By default the UI talks to a cqserver admin endpoint via the Vite
dev-server proxy at `/admin-api → http://127.0.0.1:8085`. To point at
a different instance:

```sh
VITE_ADMIN_URL=http://other-host:8085 npm run dev
```

## What's here so far (U1 + U2 + U3 of ADMIN_UI_WORKLOG.md)

- **Overview** — RSS, subscriptions, topics, publish rate, snapshot
  cache, replication. Live polling (2 s) with sparklines.
- **Topics** — AG-Grid of every registered topic with row counts,
  capacity, schema state, and quick filter.

Remaining screens (Subscriptions, Views, Queues, Replication,
Metrics, Explain, Config) are stubbed and route-mounted but show a
"coming next session" placeholder. See
`ADMIN_UI_WORKLOG.md` at the repo root.

## Production build

```sh
npm run build
```

Outputs a static bundle under `dist/`. U7 will wire this into the
cqserver process so `/ui` serves the bundle directly.

## Design system

The UI is built on the same `globals.css` + shadcn primitives as
`/Users/develop/projects/design-system/react-app`, copied locally
into `src/` so this app builds standalone.

Aesthetic: operator console — dark mode default, Inter for chrome,
JetBrains Mono for all numeric data, hairline borders, no shadows.
Motion only on value-change pulses and continuous sparklines.
