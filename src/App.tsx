import { useCallback, useEffect, useMemo, useState } from 'react';
import { chapters } from './slides';
import { SlideView } from './components/SlideView';
import { Inline } from './components/Inline';
import { load, save } from './storage';

const flat = chapters.flatMap((c, ci) => c.slides.map((s) => ({ slide: s, chapter: c, chapterIndex: ci })));

function stepCount(i: number) {
  return flat[i].slide.animation?.steps.length ?? 1;
}

function indexFromHash(): number {
  const id = decodeURIComponent(location.hash.replace(/^#\/?/, ''));
  const i = flat.findIndex((f) => f.slide.id === id);
  return i < 0 ? 0 : i;
}

type Theme = 'light' | 'dark';

function initialTheme(): Theme {
  const saved = load('theme');
  if (saved === 'light' || saved === 'dark') return saved;
  return window.matchMedia?.('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

export function App() {
  const [index, setIndex] = useState(indexFromHash);
  const [step, setStep] = useState(0);
  const [playKey, setPlayKey] = useState(0);
  const [speed, setSpeed] = useState(() => Number(load('speed')) || 1);
  const [autoplay, setAutoplay] = useState(false);
  const [sidebar, setSidebar] = useState(() => load('sidebar') !== 'closed' && window.innerWidth > 900);
  const [notes, setNotes] = useState(() => load('notes') === 'open');
  const [theme, setTheme] = useState<Theme>(initialTheme);
  const [help, setHelp] = useState(false);

  const cur = flat[index];

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    save('theme', theme);
  }, [theme]);
  useEffect(() => save('speed', String(speed)), [speed]);
  useEffect(() => save('sidebar', sidebar ? 'open' : 'closed'), [sidebar]);
  useEffect(() => save('notes', notes ? 'open' : 'closed'), [notes]);

  useEffect(() => {
    const want = `#/${cur.slide.id}`;
    if (location.hash !== want) history.replaceState(null, '', want);
    document.title = `${cur.slide.title} · Kafka Patterns Guide`;
  }, [cur]);

  useEffect(() => {
    const onHash = () => {
      setIndex(indexFromHash());
      setStep(0);
    };
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  const goSlide = useCallback((i: number, atEnd = false) => {
    if (i < 0 || i >= flat.length) return;
    setIndex(i);
    setStep(atEnd ? stepCount(i) - 1 : 0);
    setAutoplay(false);
    setPlayKey((k) => k + 1);
    if (window.innerWidth <= 900) setSidebar(false);
  }, []);

  const next = useCallback(() => {
    if (step < stepCount(index) - 1) setStep(step + 1);
    else goSlide(index + 1);
  }, [step, index, goSlide]);

  const prev = useCallback(() => {
    if (step > 0) setStep(step - 1);
    else goSlide(index - 1, true);
  }, [step, index, goSlide]);

  useEffect(() => {
    if (!autoplay) return;
    if (step >= stepCount(index) - 1) {
      setAutoplay(false);
      return;
    }
    const t = setTimeout(() => setStep((s) => s + 1), 2600 / speed);
    return () => clearTimeout(t);
  }, [autoplay, step, index, speed]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      const tag = (e.target as HTMLElement).tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA') return;
      switch (e.key) {
        case 'ArrowRight':
        case 'PageDown':
        case ' ':
          e.preventDefault();
          next();
          break;
        case 'ArrowLeft':
        case 'PageUp':
          e.preventDefault();
          prev();
          break;
        case 'ArrowDown':
          e.preventDefault();
          goSlide(index + 1);
          break;
        case 'ArrowUp':
          e.preventDefault();
          goSlide(index - 1);
          break;
        case 'Home':
          goSlide(0);
          break;
        case 'End':
          goSlide(flat.length - 1);
          break;
        case 'n':
          setNotes((v) => !v);
          break;
        case 'm':
          setSidebar((v) => !v);
          break;
        case 't':
          setTheme((v) => (v === 'dark' ? 'light' : 'dark'));
          break;
        case 'p':
          setAutoplay((v) => !v);
          break;
        case 'r':
          setPlayKey((k) => k + 1);
          break;
        case 'f':
          if (document.fullscreenElement) void document.exitFullscreen();
          else void document.documentElement.requestFullscreen?.();
          break;
        case '?':
          setHelp((v) => !v);
          break;
        case 'Escape':
          setHelp(false);
          break;
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [next, prev, goSlide, index]);

  const progress = useMemo(() => ((index + 1) / flat.length) * 100, [index]);

  return (
    <div className={`app ${sidebar ? 'with-sidebar' : ''} ${notes ? 'with-notes' : ''}`}>
      <header className="topbar">
        <button className="icon-btn" onClick={() => setSidebar((v) => !v)} aria-label="Toggle contents" title="Contents (M)">
          ☰
        </button>
        <div className="brand">
          Kafka Patterns <span>for Investment Banking</span>
        </div>
        <div className="crumb">
          {cur.chapter.title} · <strong>{cur.slide.title}</strong>
        </div>
        <div className="topbar-actions">
          <span className="counter">
            {index + 1} / {flat.length}
          </span>
          <button className="icon-btn" onClick={() => setNotes((v) => !v)} aria-pressed={notes} title="Speaker notes (N)">
            Notes
          </button>
          <button className="icon-btn" onClick={() => setTheme((v) => (v === 'dark' ? 'light' : 'dark'))} title="Theme (T)" aria-label="Toggle theme">
            {theme === 'dark' ? '☀' : '☾'}
          </button>
          <button className="icon-btn" onClick={() => setHelp((v) => !v)} title="Keyboard shortcuts (?)" aria-label="Keyboard shortcuts">
            ?
          </button>
        </div>
        <div className="progress" style={{ width: `${progress}%` }} />
      </header>

      <nav className="sidebar" aria-label="Contents">
        {chapters.map((c, ci) => (
          <div key={c.id} className="toc-chapter">
            <div className={`toc-chapter-title ${cur.chapterIndex === ci ? 'current' : ''}`}>{c.title}</div>
            <ol>
              {c.slides.map((s) => {
                const i = flat.findIndex((f) => f.slide.id === s.id);
                return (
                  <li key={s.id}>
                    <a
                      href={`#/${s.id}`}
                      className={i === index ? 'active' : ''}
                      onClick={(e) => {
                        e.preventDefault();
                        goSlide(i);
                      }}
                    >
                      {s.title}
                    </a>
                  </li>
                );
              })}
            </ol>
          </div>
        ))}
      </nav>

      <main className="stage">
        <SlideView
          key={cur.slide.id}
          slide={cur.slide}
          chapterTitle={cur.chapter.title}
          step={step}
          playKey={playKey}
          speed={speed}
          autoplay={autoplay}
          onStep={(s) => {
            setStep(Math.max(0, Math.min(stepCount(index) - 1, s)));
            setAutoplay(false);
          }}
          onReplay={() => setPlayKey((k) => k + 1)}
          onToggleAutoplay={() => {
            if (!autoplay && step >= stepCount(index) - 1) setStep(0);
            setAutoplay((v) => !v);
          }}
          onSpeed={setSpeed}
        />
        <div className="nav-arrows">
          <button onClick={prev} disabled={index === 0 && step === 0} aria-label="Back">
            ←
          </button>
          <button onClick={next} disabled={index === flat.length - 1 && step === stepCount(index) - 1} aria-label="Forward">
            →
          </button>
        </div>
      </main>

      {notes && (
        <aside className="notes" aria-label="Speaker notes">
          <div className="notes-title">Speaker notes</div>
          {cur.slide.notes.split('\n\n').map((para, i) => (
            <p key={i}>
              <Inline text={para} />
            </p>
          ))}
        </aside>
      )}

      {help && (
        <div className="help-overlay" onClick={() => setHelp(false)}>
          <div className="help" onClick={(e) => e.stopPropagation()} role="dialog" aria-label="Keyboard shortcuts">
            <h3>Keyboard shortcuts</h3>
            <dl>
              <dt>→ / Space</dt>
              <dd>Next animation step, then next slide</dd>
              <dt>←</dt>
              <dd>Previous step / slide</dd>
              <dt>↓ / ↑</dt>
              <dd>Next / previous slide (skip steps)</dd>
              <dt>P</dt>
              <dd>Autoplay steps</dd>
              <dt>R</dt>
              <dd>Replay current step</dd>
              <dt>N</dt>
              <dd>Speaker notes</dd>
              <dt>M</dt>
              <dd>Contents sidebar</dd>
              <dt>T</dt>
              <dd>Light / dark theme</dd>
              <dt>F</dt>
              <dd>Fullscreen</dd>
            </dl>
            <button onClick={() => setHelp(false)}>Close</button>
          </div>
        </div>
      )}
    </div>
  );
}
