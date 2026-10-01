import { useMemo } from 'react';
import type { Slide } from '../engine/types';
import { resolveSteps } from '../engine/resolve';
import { SceneView } from '../engine/SceneView';
import { CodeBlock } from './CodeBlock';
import { BarChart } from './BarChart';
import { Inline } from './Inline';

const CALLOUT_LABEL = {
  misconception: 'Common misconception',
  rule: 'Rule',
  tip: 'Tip',
  warning: 'Watch out',
} as const;

interface Props {
  slide: Slide;
  chapterTitle: string;
  step: number;
  playKey: number;
  speed: number;
  autoplay: boolean;
  onStep: (s: number) => void;
  onReplay: () => void;
  onToggleAutoplay: () => void;
  onSpeed: (s: number) => void;
}

export function SlideView(p: Props) {
  const { slide } = p;
  const steps = useMemo(() => (slide.animation ? resolveSteps(slide.animation.steps) : []), [slide]);

  if (slide.layout === 'title' || slide.layout === 'section') {
    return (
      <section className={`slide slide-${slide.layout}`}>
        <div className="title-wrap">
          {slide.layout === 'section' && <div className="eyebrow">{p.chapterTitle}</div>}
          <h1>{slide.title}</h1>
          {slide.subtitle && <p className="subtitle">{slide.subtitle}</p>}
          {slide.bullets && (
            <ul className="title-bullets">
              {slide.bullets.map((b, i) => (
                <li key={i}>
                  <Inline text={b} />
                </li>
              ))}
            </ul>
          )}
        </div>
      </section>
    );
  }

  const hasVisual = !!(slide.animation || slide.chart);
  const layoutClass = hasVisual ? 'has-visual' : slide.code ? 'no-visual' : 'single';
  const step = Math.min(p.step, Math.max(steps.length - 1, 0));
  const cur = steps[step];

  return (
    <section className={`slide slide-standard ${layoutClass}`}>
      <header className="slide-head">
        <div className="eyebrow">{p.chapterTitle}</div>
        <h2>{slide.title}</h2>
        {slide.subtitle && <p className="subtitle">{slide.subtitle}</p>}
      </header>
      <div className="slide-body">
        <div className="col-text">
          {slide.bullets && (
            <ul className="bullets">
              {slide.bullets.map((b, i) => (
                <li key={i}>
                  <Inline text={b} />
                </li>
              ))}
            </ul>
          )}
          {slide.callout && (
            <aside className={`callout callout-${slide.callout.kind}`}>
              <div className="callout-kind">{CALLOUT_LABEL[slide.callout.kind]}</div>
              <div className="callout-title">
                <Inline text={slide.callout.title} />
              </div>
              <p>
                <Inline text={slide.callout.text} />
              </p>
            </aside>
          )}
          {slide.table && (
            <table className="compare">
              <thead>
                <tr>
                  {slide.table.head.map((h) => (
                    <th key={h}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {slide.table.rows.map((r, i) => (
                  <tr key={i}>
                    {r.map((c, j) => (
                      <td key={j}>
                        <Inline text={c} />
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
        {layoutClass !== 'single' && <div className="col-visual">
          {slide.animation && cur && (
            <div className="anim">
              <div className="anim-stage">
                <SceneView
                  scene={slide.animation.scene}
                  step={cur}
                  prevStep={step > 0 ? steps[step - 1] : undefined}
                  stepIndex={step}
                  playKey={p.playKey}
                  speed={p.speed}
                />
              </div>
              <div className="anim-caption" aria-live="polite">
                <span className="step-no">
                  {step + 1}/{steps.length}
                </span>
                <span>
                  <Inline text={cur.caption} />
                </span>
              </div>
              <div className="anim-controls">
                <button onClick={() => p.onStep(step - 1)} disabled={step === 0} aria-label="Previous step" title="Previous step (←)">
                  ‹
                </button>
                <div className="step-dots">
                  {steps.map((_, i) => (
                    <button
                      key={i}
                      className={`dot ${i === step ? 'on' : ''} ${i < step ? 'past' : ''}`}
                      onClick={() => p.onStep(i)}
                      aria-label={`Step ${i + 1}`}
                    />
                  ))}
                </div>
                <button onClick={() => p.onStep(step + 1)} disabled={step === steps.length - 1} aria-label="Next step" title="Next step (→)">
                  ›
                </button>
                <span className="sep" />
                <button onClick={p.onReplay} title="Replay step (R)">
                  ↻
                </button>
                <button onClick={p.onToggleAutoplay} className={p.autoplay ? 'on' : ''} title="Autoplay (P)">
                  {p.autoplay ? '❚❚' : '▶'}
                </button>
                <label className="speed" title="Animation speed">
                  <span>{p.speed}×</span>
                  <input type="range" min={0.5} max={2} step={0.25} value={p.speed} onChange={(e) => p.onSpeed(Number(e.target.value))} />
                </label>
              </div>
            </div>
          )}
          {slide.chart && <BarChart chart={slide.chart} />}
          {slide.code && <CodeBlock code={slide.code} />}
        </div>}
      </div>
    </section>
  );
}
