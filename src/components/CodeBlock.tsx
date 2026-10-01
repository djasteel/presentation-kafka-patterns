import type { ReactNode } from 'react';
import type { CodeSample } from '../engine/types';

const KEYWORDS: Record<CodeSample['lang'], RegExp | null> = {
  java: /\b(public|private|protected|static|final|void|class|interface|record|return|new|if|else|for|while|try|catch|finally|throw|throws|import|package|extends|implements|var|int|long|boolean|this|null|true|false|switch|case|default|break|continue)\b/,
  sql: /\b(SELECT|FROM|WHERE|INSERT|INTO|VALUES|UPDATE|SET|DELETE|CREATE|TABLE|INDEX|UNIQUE|PRIMARY|KEY|NOT|NULL|ON|CONFLICT|DO|NOTHING|ORDER|BY|LIMIT|FOR|SKIP|LOCKED|GROUP|MAX|AND|OR|AS|IN|RETURNING|BIGINT|TEXT|INT|TIMESTAMPTZ|JSONB|DEFAULT|now|INCLUDE|USING|WITH|BEGIN|COMMIT|EXISTS|IS)\b/,
  yaml: null,
  bash: /\b(kafka-consumer-groups|psql|--[a-z-]+)\b/,
  text: null,
};

const TOKEN =
  /(\/\/[^\n]*|--\s[^\n]*|#[^\n]*|"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|@\w+|\b\d[\d_.]*\b|\b[A-Za-z_][\w-]*\b|[^\w\s]+|\s+)/g;

function highlight(src: string, lang: CodeSample['lang']): ReactNode[] {
  if (lang === 'text') return [src];
  const kw = KEYWORDS[lang];
  const out: ReactNode[] = [];
  let i = 0;
  for (const m of src.matchAll(TOKEN)) {
    const t = m[0];
    let cls: string | undefined;
    if (t.startsWith('//') || (lang === 'sql' && t.startsWith('-- ')) || ((lang === 'yaml' || lang === 'bash') && t.startsWith('#'))) cls = 'c-comment';
    else if (t.startsWith('"') || t.startsWith("'")) cls = 'c-string';
    else if (t.startsWith('@') && lang === 'java') cls = 'c-annot';
    else if (/^\d/.test(t)) cls = 'c-num';
    else if (kw && kw.test(t) && new RegExp(`^(?:${kw.source})$`).test(t)) cls = 'c-kw';
    else if (lang === 'yaml' && /^[\w.-]+$/.test(t) && src[(m.index ?? 0) + t.length] === ':') cls = 'c-key';
    else if (lang === 'java' && /^[A-Z][A-Za-z0-9]+$/.test(t)) cls = 'c-type';
    out.push(cls ? <span key={i++} className={cls}>{t}</span> : t);
  }
  return out;
}

export function CodeBlock({ code }: { code: CodeSample }) {
  return (
    <figure className="code">
      {code.caption && <figcaption>{code.caption}</figcaption>}
      <pre>
        <code>{highlight(code.src.trim(), code.lang)}</code>
      </pre>
    </figure>
  );
}
