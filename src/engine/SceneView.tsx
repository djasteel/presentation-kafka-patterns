import { motion, useReducedMotion } from 'motion/react';
import type { ResolvedStep, Scene, SceneEdge, SceneLog, SceneNode, SceneTable, Row } from './types';

const KIND_TAG: Partial<Record<SceneNode['kind'], string>> = {
  producer: 'PRODUCER',
  consumer: 'CONSUMER',
  topic: 'TOPIC',
  db: '',
  service: 'SERVICE',
  dlt: 'DLT',
  thread: '',
};

interface Pt {
  x: number;
  y: number;
}

function center(n: SceneNode): Pt {
  return { x: n.x + n.w / 2, y: n.y + n.h / 2 };
}

/** Point where the ray from the node centre towards `toward` leaves the node's rectangle. */
function boundary(n: SceneNode, toward: Pt, pad = 3): Pt {
  const c = center(n);
  const dx = toward.x - c.x;
  const dy = toward.y - c.y;
  if (dx === 0 && dy === 0) return c;
  const hw = n.w / 2 + pad;
  const hh = n.h / 2 + pad;
  const t = Math.min(dx !== 0 ? hw / Math.abs(dx) : Infinity, dy !== 0 ? hh / Math.abs(dy) : Infinity);
  return { x: c.x + dx * t, y: c.y + dy * t };
}

interface EdgeGeom {
  p0: Pt;
  p1: Pt;
  p2: Pt;
}

function edgeGeom(e: SceneEdge, byId: Map<string, SceneNode>): EdgeGeom | null {
  const a = byId.get(e.from);
  const b = byId.get(e.to);
  if (!a || !b) return null;
  const ca = center(a);
  const cb = center(b);
  const mid = { x: (ca.x + cb.x) / 2, y: (ca.y + cb.y) / 2 };
  const len = Math.hypot(cb.x - ca.x, cb.y - ca.y) || 1;
  const nx = -(cb.y - ca.y) / len;
  const ny = (cb.x - ca.x) / len;
  const ctrl = { x: mid.x + nx * (e.curve ?? 0), y: mid.y + ny * (e.curve ?? 0) };
  const p0 = boundary(a, e.curve ? ctrl : cb);
  const p2 = boundary(b, e.curve ? ctrl : ca, 6);
  const p1 = e.curve ? ctrl : { x: (p0.x + p2.x) / 2, y: (p0.y + p2.y) / 2 };
  return { p0, p1, p2 };
}

function bezier(g: EdgeGeom, t: number): Pt {
  const u = 1 - t;
  return {
    x: u * u * g.p0.x + 2 * u * t * g.p1.x + t * t * g.p2.x,
    y: u * u * g.p0.y + 2 * u * t * g.p1.y + t * t * g.p2.y,
  };
}

function pathD(g: EdgeGeom): string {
  return `M ${g.p0.x} ${g.p0.y} Q ${g.p1.x} ${g.p1.y} ${g.p2.x} ${g.p2.y}`;
}

function NodeShape({ n, tone, badge }: { n: SceneNode; tone?: string; badge?: string }) {
  const cls = `node node-${n.kind} tone-${tone ?? 'default'}`;
  const c = center(n);
  if (n.kind === 'region') {
    return (
      <g className={cls}>
        <rect x={n.x} y={n.y} width={n.w} height={n.h} rx={14} className="region-box" />
        <text x={n.x + 12} y={n.y + 20} className="region-label">
          {n.label}
        </text>
        {n.sub && (
          <text x={n.x + 12} y={n.y + 36} className="node-sub">
            {n.sub}
          </text>
        )}
        {badge && <Badge x={n.x + n.w - 12} y={n.y + 12} text={badge} anchor="end" />}
      </g>
    );
  }
  if (n.kind === 'note') {
    return (
      <g className={cls}>
        <text x={c.x} y={c.y} textAnchor="middle" dominantBaseline="middle" className="note-text">
          {n.label}
        </text>
      </g>
    );
  }
  const tag = KIND_TAG[n.kind];
  return (
    <motion.g className={cls} animate={{ opacity: tone === 'muted' ? 0.4 : 1 }}>
      {n.kind === 'db' ? (
        <>
          <path
            className="shape"
            d={`M ${n.x} ${n.y + 8} a ${n.w / 2} 8 0 0 0 ${n.w} 0 v ${n.h - 16} a ${n.w / 2} 8 0 0 1 ${-n.w} 0 z`}
          />
          <ellipse className="shape db-top" cx={c.x} cy={n.y + 8} rx={n.w / 2} ry={8} />
        </>
      ) : (
        <rect className="shape" x={n.x} y={n.y} width={n.w} height={n.h} rx={n.kind === 'thread' ? 6 : 10} />
      )}
      {n.kind === 'topic' && (
        <g className="topic-stripes">
          {[1, 2, 3].map((i) => (
            <line key={i} x1={n.x + 8} x2={n.x + n.w - 8} y1={n.y + (n.h * i) / 4} y2={n.y + (n.h * i) / 4} />
          ))}
        </g>
      )}
      {tag && n.h >= 44 && (
        <text x={c.x} y={n.y + 13} textAnchor="middle" className="node-tag">
          {tag}
        </text>
      )}
      <text
        x={c.x}
        y={n.kind === 'db' ? c.y + 5 : tag && n.h >= 44 ? c.y + 4 : c.y + (n.sub ? -2 : 4)}
        textAnchor="middle"
        className="node-label"
      >
        {n.label}
      </text>
      {n.sub && (
        <text
          x={c.x}
          y={n.kind === 'db' ? c.y + 19 : tag && n.h >= 44 ? c.y + 18 : c.y + 12}
          textAnchor="middle"
          className="node-sub"
        >
          {n.sub}
        </text>
      )}
      {tone === 'fail' && (
        <g className="fail-cross">
          <circle cx={n.x + n.w - 2} cy={n.y + 2} r={9} />
          <path d={`M ${n.x + n.w - 6} ${n.y - 2} l 8 8 M ${n.x + n.w + 2} ${n.y - 2} l -8 8`} />
        </g>
      )}
      {badge && <Badge x={c.x} y={n.y + n.h + 14} text={badge} anchor="middle" />}
    </motion.g>
  );
}

function Badge({ x, y, text, anchor }: { x: number; y: number; text: string; anchor: 'middle' | 'end' }) {
  const w = text.length * 7 + 16;
  const left = anchor === 'middle' ? x - w / 2 : x - w;
  return (
    <motion.g className="badge" initial={{ opacity: 0, y: -4 }} animate={{ opacity: 1, y: 0 }} key={text}>
      <rect x={left} y={y - 10} width={w} height={20} rx={10} />
      <text x={left + w / 2} y={y + 4} textAnchor="middle">
        {text}
      </text>
    </motion.g>
  );
}

function TableView({ t, rows, prevRows }: { t: SceneTable; rows: Row[]; prevRows: Row[] }) {
  const rowH = 22;
  const max = t.maxRows ?? 6;
  const hidden = Math.max(0, rows.length - max);
  const visible = rows.slice(hidden);
  const widths = t.widths ?? t.columns.map(() => 1);
  const total = widths.reduce((a, b) => a + b, 0);
  const xs: number[] = [];
  let acc = t.x;
  for (const w of widths) {
    xs.push(acc);
    acc += (w / total) * t.w;
  }
  const prevKeys = new Set(prevRows.map((r) => r.cells.join('|')));
  const height = 26 + rowH * (Math.max(visible.length, 1) + (hidden ? 1 : 0)) + 6;
  return (
    <g className="table">
      <text x={t.x} y={t.y - 6} className="table-title">
        {t.title}
      </text>
      <rect x={t.x} y={t.y} width={t.w} height={height} rx={6} className="table-bg" />
      <rect x={t.x} y={t.y} width={t.w} height={24} rx={6} className="table-head" />
      {t.columns.map((c, i) => (
        <text key={c} x={xs[i] + 6} y={t.y + 16.5} className="table-col">
          {c}
        </text>
      ))}
      {hidden > 0 && (
        <text x={t.x + 6} y={t.y + 26 + rowH - 6} className="table-more">
          … {hidden} earlier row{hidden > 1 ? 's' : ''}
        </text>
      )}
      {visible.length === 0 && (
        <text x={t.x + 6} y={t.y + 26 + rowH - 6} className="table-more">
          (empty)
        </text>
      )}
      {visible.map((r, ri) => {
        const y = t.y + 26 + rowH * (ri + (hidden ? 1 : 0));
        const key = r.cells.join('|');
        const isNew = !prevKeys.has(key);
        return (
          <g key={key + ri} className={`table-row tone-${r.tone ?? 'default'}`}>
            <motion.rect
              x={t.x + 2}
              y={y}
              width={t.w - 4}
              height={rowH - 2}
              rx={3}
              className="row-bg"
              initial={isNew ? { opacity: 1 } : false}
              animate={{ opacity: r.tone && r.tone !== 'default' ? 1 : 0 }}
              transition={{ duration: 1.6 }}
            />
            {r.cells.map((c, ci) => (
              <motion.text
                key={ci}
                x={xs[ci] + 6}
                y={y + 15}
                className="table-cell"
                initial={isNew ? { opacity: 0 } : false}
                animate={{ opacity: 1 }}
              >
                {c}
              </motion.text>
            ))}
            {r.tone === 'dup' && <line x1={t.x + 6} x2={t.x + t.w - 6} y1={y + 10} y2={y + 10} className="strike" />}
          </g>
        );
      })}
    </g>
  );
}

function LogView({ l, state }: { l: SceneLog; state?: ResolvedStep['logs'][string] }) {
  const cw = l.cellW ?? 34;
  const h = 30;
  const base = l.base ?? 0;
  const committed = state?.committed;
  return (
    <g className="log">
      <text x={l.x} y={l.y - 8} className="table-title">
        {l.title}
      </text>
      {l.cells.map((c, i) => {
        const tone = state?.marks?.[i + base] ?? 'default';
        return (
          <g key={i} className={`log-cell tone-${tone}`}>
            <motion.rect
              x={l.x + i * cw}
              y={l.y}
              width={cw - 3}
              height={h}
              rx={4}
              className="cell-bg"
              layout
            />
            <text x={l.x + i * cw + (cw - 3) / 2} y={l.y + 19.5} textAnchor="middle" className="cell-text">
              {c}
            </text>
            {l.showOffsets !== false && c !== String(i + base) && (
              <text x={l.x + i * cw + (cw - 3) / 2} y={l.y + h + 13} textAnchor="middle" className="cell-offset">
                {i + base}
              </text>
            )}
          </g>
        );
      })}
      {committed !== undefined && (
        <motion.g
          className="committed"
          initial={false}
          animate={{ x: l.x + (committed - base) * cw - 2.5 }}
          transition={{ type: 'spring', stiffness: 120, damping: 18 }}
        >
          <line x1={0} x2={0} y1={l.y - 6} y2={l.y + h + 20} />
          <text x={0} y={l.y + h + 34} textAnchor="middle">
            {state?.committedLabel ?? `committed=${committed}`}
          </text>
        </motion.g>
      )}
    </g>
  );
}

function Message({
  geom,
  label,
  tone,
  delay,
  outcome,
  duration,
}: {
  geom: EdgeGeom;
  label: string;
  tone: string;
  delay: number;
  outcome: 'arrive' | 'reject' | 'stop';
  duration: number;
}) {
  const end = outcome === 'stop' ? 0.5 : 1;
  const N = 14;
  const pts = Array.from({ length: N + 1 }, (_, i) => bezier(geom, (i / N) * end));
  const w = label.length * 7.4 + 16;
  // 'arrive' fades on arrival; 'reject' lingers briefly to show the cross, then fades; 'stop' stays put.
  const fadeDuration = outcome === 'reject' ? duration + 1.4 : duration * 1.15;
  const fadeTimes =
    outcome === 'arrive' ? [0, 0.1, 0.85, 1] : outcome === 'reject' ? [0, 0.08, 0.8, 1] : [0, 0.1, 1, 1];
  const opacity = outcome === 'stop' ? [0, 1, 1, 1] : [0, 1, 1, 0];
  return (
    <motion.g
      className={`msg tone-${tone}`}
      initial={{ x: pts[0].x, y: pts[0].y, opacity: 0 }}
      animate={{ x: pts.map((p) => p.x), y: pts.map((p) => p.y), opacity }}
      transition={{
        x: { duration, delay, ease: 'easeInOut' },
        y: { duration, delay, ease: 'easeInOut' },
        opacity: { duration: fadeDuration, delay, times: fadeTimes },
      }}
    >
      <rect x={-w / 2} y={-11} width={w} height={22} rx={11} />
      <text y={4.5} textAnchor="middle">
        {label}
      </text>
      {outcome !== 'arrive' && (
        <motion.g
          className="msg-x"
          initial={{ scale: 0, opacity: 0 }}
          animate={{ scale: 1, opacity: 1 }}
          transition={{ delay: delay + duration, duration: 0.25 }}
        >
          <circle cx={w / 2} cy={-10} r={8} />
          <path d={`M ${w / 2 - 3.5} -13.5 l 7 7 M ${w / 2 + 3.5} -13.5 l -7 7`} />
        </motion.g>
      )}
    </motion.g>
  );
}

export function SceneView({
  scene,
  step,
  prevStep,
  stepIndex,
  playKey,
  speed,
}: {
  scene: Scene;
  step: ResolvedStep;
  prevStep?: ResolvedStep;
  stepIndex: number;
  playKey: number;
  speed: number;
}) {
  const reduced = useReducedMotion();
  const width = scene.width ?? 800;
  const height = scene.height ?? 460;
  const byId = new Map(scene.nodes.map((n) => [n.id, n]));
  const geoms = new Map<string, EdgeGeom>();
  for (const e of scene.edges ?? []) {
    const g = edgeGeom(e, byId);
    if (g) geoms.set(e.id, g);
  }
  const base = reduced ? 0.01 : 1.1 / speed;
  const regions = scene.nodes.filter((n) => n.kind === 'region');
  const others = scene.nodes.filter((n) => n.kind !== 'region');

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="scene" role="img" aria-label={step.caption}>
      <defs>
        <marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M 0 0 L 10 5 L 0 10 z" className="arrow-head" />
        </marker>
        <marker id="arrow-fail" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M 0 0 L 10 5 L 0 10 z" className="arrow-head-fail" />
        </marker>
      </defs>
      {regions.map((n) => (
        <NodeShape key={n.id} n={n} tone={step.nodes[n.id]?.tone} badge={step.nodes[n.id]?.badge} />
      ))}
      {(scene.edges ?? []).map((e) => {
        const g = geoms.get(e.id);
        const st = step.edges[e.id];
        if (!g || st?.hidden) return null;
        const mid = bezier(g, 0.5);
        const fail = st?.tone === 'fail';
        return (
          <g key={e.id} className={`edge tone-${st?.tone ?? 'default'}`}>
            <path d={pathD(g)} className={e.dashed ? 'dashed' : ''} markerEnd={fail ? 'url(#arrow-fail)' : 'url(#arrow)'} />
            {e.label && (
              <text x={mid.x} y={mid.y - 7} textAnchor="middle" className="edge-label">
                {e.label}
              </text>
            )}
          </g>
        );
      })}
      {others.map((n) => (
        <NodeShape key={n.id} n={n} tone={step.nodes[n.id]?.tone} badge={step.nodes[n.id]?.badge} />
      ))}
      {(scene.tables ?? []).map((t) => (
        <TableView key={t.id} t={t} rows={step.tables[t.id] ?? []} prevRows={prevStep?.tables[t.id] ?? step.tables[t.id] ?? []} />
      ))}
      {(scene.logs ?? []).map((l) => (
        <LogView key={l.id} l={l} state={step.logs[l.id]} />
      ))}
      {step.messages.map((m, i) => {
        const g = geoms.get(m.edge);
        if (!g) return null;
        return (
          <Message
            key={`${stepIndex}-${playKey}-${i}`}
            geom={g}
            label={m.label}
            tone={m.tone ?? 'default'}
            delay={(m.delay ?? 0) * 0.35 * base}
            outcome={m.outcome ?? 'arrive'}
            duration={base}
          />
        );
      })}
    </svg>
  );
}
