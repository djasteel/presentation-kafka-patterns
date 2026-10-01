import type { Row, Tone } from '../engine/types';

export const r = (...cells: string[]): Row => ({ cells });
export const rt = (tone: Tone, ...cells: string[]): Row => ({ cells, tone });

/** Marks for a contiguous range of log offsets. */
export function range(from: number, to: number, tone: Tone): Record<number, Tone> {
  const out: Record<number, Tone> = {};
  for (let i = from; i <= to; i++) out[i] = tone;
  return out;
}
