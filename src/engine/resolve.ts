import type { ResolvedStep, Step } from './types';

/** Tables, logs, nodes and edges carry over from the previous step unless overridden by id. */
export function resolveSteps(steps: Step[]): ResolvedStep[] {
  const out: ResolvedStep[] = [];
  let prev: ResolvedStep = { caption: '', messages: [], tables: {}, logs: {}, nodes: {}, edges: {} };
  for (const s of steps) {
    const next: ResolvedStep = {
      caption: s.caption,
      messages: s.messages ?? [],
      tables: { ...prev.tables, ...s.tables },
      logs: { ...prev.logs, ...s.logs },
      nodes: { ...prev.nodes, ...s.nodes },
      edges: { ...prev.edges, ...s.edges },
    };
    out.push(next);
    prev = next;
  }
  return out;
}
