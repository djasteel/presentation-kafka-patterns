import { useState } from 'react';
import type { ChartKey } from '../engine/types';
import perf from '../data/perf-data.json';

interface Dataset {
  placeholder: boolean;
  title: string;
  unit: string;
  note?: string;
  bars: { label: string; value: number }[];
}

const fmt = new Intl.NumberFormat('en-US');

export function BarChart({ chart }: { chart: ChartKey }) {
  const data = (perf as unknown as Record<ChartKey, Dataset>)[chart];
  const [hover, setHover] = useState<number | null>(null);
  const max = Math.max(...data.bars.map((b) => b.value));
  const labelW = 250;
  const rowH = 44;
  const plotW = 330;
  const width = labelW + plotW + 90;
  const height = data.bars.length * rowH + 30;
  // Rounded "nice" gridline step.
  const rawStep = max / 4;
  const mag = 10 ** Math.floor(Math.log10(rawStep));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => s >= rawStep) ?? rawStep;
  const ticks = Array.from({ length: Math.floor(max / step) + 1 }, (_, i) => i * step);
  const scaleMax = Math.max(ticks[ticks.length - 1], max);
  const x = (v: number) => (v / scaleMax) * plotW;

  return (
    <figure className="chart">
      <figcaption>
        <span className="chart-title">{data.title}</span>
        <span className="chart-unit">{data.unit}</span>
      </figcaption>
      {data.placeholder && (
        <div className="placeholder-banner" role="note">
          Placeholder values — replace with measured numbers in <code>src/data/perf-data.json</code>
        </div>
      )}
      <svg viewBox={`0 0 ${width} ${height}`} className={data.placeholder ? 'is-placeholder' : ''} role="img" aria-label={data.title}>
        <defs>
          <pattern id="hatch" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
            <rect width="6" height="6" className="hatch-bg" />
            <line x1="0" y1="0" x2="0" y2="6" className="hatch-line" />
          </pattern>
        </defs>
        {ticks.map((t) => (
          <g key={t} className="grid">
            <line x1={labelW + x(t)} x2={labelW + x(t)} y1={0} y2={height - 22} />
            <text x={labelW + x(t)} y={height - 6} textAnchor="middle">
              {fmt.format(t)}
            </text>
          </g>
        ))}
        {data.bars.map((b, i) => {
          const y = i * rowH + 8;
          const bw = Math.max(x(b.value), 2);
          return (
            <g
              key={b.label}
              className={`bar-row ${hover === i ? 'hover' : ''} ${hover !== null && hover !== i ? 'dim' : ''}`}
              onMouseEnter={() => setHover(i)}
              onMouseLeave={() => setHover(null)}
            >
              <rect x={0} y={y - 6} width={width} height={rowH} className="hit" />
              <text x={labelW - 12} y={y + 18} textAnchor="end" className="bar-label">
                {b.label}
              </text>
              <path
                className="bar"
                fill={data.placeholder ? 'url(#hatch)' : undefined}
                d={`M ${labelW} ${y + 4} h ${bw - 4} a 4 4 0 0 1 4 4 v 18 a 4 4 0 0 1 -4 4 h ${-(bw - 4)} z`}
              />
              <text x={labelW + bw + 8} y={y + 18} className="bar-value">
                {fmt.format(b.value)}
              </text>
            </g>
          );
        })}
      </svg>
      <div className="chart-tooltip" aria-live="polite">
        {hover !== null ? (
          <>
            <strong>{data.bars[hover].label}</strong>: {fmt.format(data.bars[hover].value)} {data.unit}
            {data.placeholder && ' (placeholder)'}
          </>
        ) : (
          data.note
        )}
      </div>
      <details className="chart-table">
        <summary>Table view</summary>
        <table>
          <thead>
            <tr>
              <th>Scenario</th>
              <th>{data.unit}</th>
            </tr>
          </thead>
          <tbody>
            {data.bars.map((b) => (
              <tr key={b.label}>
                <td>{b.label}</td>
                <td>{fmt.format(b.value)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </details>
    </figure>
  );
}
