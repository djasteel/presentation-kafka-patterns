import type { Chapter } from '../engine/types';
import { intro } from './intro';
import { consumptionA } from './ch1-consumption';
import { consumptionB } from './ch1-errors';
import { consumptionC } from './ch1-observability';
import { publishing } from './ch2-publishing';
import { resiliency } from './ch3-resiliency';
import { closing } from './ch4-closing';

export const chapters: Chapter[] = [
  intro,
  { id: 'consumption', title: '1 · Kafka consumption', slides: [...consumptionA, ...consumptionB, ...consumptionC] },
  publishing,
  resiliency,
  closing,
];

const ids = chapters.flatMap((c) => c.slides.map((s) => s.id));
const dupes = ids.filter((id, i) => ids.indexOf(id) !== i);
if (dupes.length) throw new Error(`Duplicate slide ids: ${dupes.join(', ')}`);
