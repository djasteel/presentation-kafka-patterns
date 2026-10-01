# Kafka Patterns for Investment Banking

An animated web presentation and reference guide: which Kafka patterns to use in investment-bank applications, and how to compensate for Kafka's delivery-semantics limitations. Requirements: [`initial-intent-normalized.md`](initial-intent-normalized.md).

## Run

```bash
npm install
npm run dev            # http://localhost:5173
```

## Build

```bash
npm run build          # dist/          -> upload to any static host (GitLab Pages, Azure Static Web Apps)
npm run build:single   # dist-single/index.html -> one self-contained file, opens from disk / offline
```

Routing is hash-based (`#/<slide-id>`), so every slide has a stable deep link and no server rewrites are needed.

## Presenting

| Key | Action |
|---|---|
| → / Space | Next animation step, then next slide |
| ← | Previous step / slide |
| ↓ / ↑ | Next / previous slide (skip steps) |
| P | Autoplay steps |
| R | Replay current step |
| N | Speaker notes |
| M | Contents sidebar |
| T | Light / dark theme |
| F | Fullscreen |
| ? | Shortcut help |

## Performance numbers

All charts read `src/data/perf-data.json`. The shipped values are **placeholders**: replace them with your measured numbers and set `"placeholder": false` per chart to remove the banner and hatching.

## Structure

```
src/
  engine/        declarative scene model (types.ts), step resolution, SVG renderer (SceneView.tsx)
  components/    slide layout, code highlighting, bar chart
  slides/        content: one file per chapter; each slide = text + optional code + animation/chart + notes
  data/          perf-data.json
```

### Adding or editing a slide

A slide is plain data (`Slide` in `src/engine/types.ts`). An animation is a `scene` (nodes, edges, tables, partition logs at fixed coordinates in an 800-wide viewBox) plus `steps`. Each step has a caption and may:

- send `messages` along edges (`outcome: 'arrive' | 'reject' | 'stop'`),
- replace table rows (`tables`), move a log's committed marker or colour its cells (`logs`),
- change node / edge tones and badges (`nodes`, `edges`).

Tables, logs, nodes and edges carry over from the previous step unless overridden, so each step only states what changes.
