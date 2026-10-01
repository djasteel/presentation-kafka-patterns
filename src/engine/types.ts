// Declarative model for animated slides.
// A Scene is static geometry; each Step describes what changes when the presenter advances.

export type Tone = 'default' | 'active' | 'ok' | 'fail' | 'warn' | 'muted' | 'dup' | 'parked' | 'new';

export type NodeKind =
  | 'producer'
  | 'consumer'
  | 'topic'
  | 'db'
  | 'service'
  | 'dlt'
  | 'region'
  | 'thread'
  | 'note';

export interface SceneNode {
  id: string;
  kind: NodeKind;
  label: string;
  sub?: string;
  x: number;
  y: number;
  w: number;
  h: number;
}

export interface SceneEdge {
  id: string;
  from: string;
  to: string;
  label?: string;
  dashed?: boolean;
  /** Bend the arrow: perpendicular offset of the control point, in scene units. */
  curve?: number;
}

export interface SceneTable {
  id: string;
  title: string;
  x: number;
  y: number;
  w: number;
  columns: string[];
  /** Relative column widths; defaults to equal. */
  widths?: number[];
  maxRows?: number;
}

/** A partition log: a row of offset cells with a committed-offset marker. */
export interface SceneLog {
  id: string;
  title: string;
  x: number;
  y: number;
  cellW?: number;
  cells: string[];
  /** Offset of the first cell, used for labels. */
  base?: number;
  /** Show offset numbers under cells (default true). */
  showOffsets?: boolean;
}

export interface Scene {
  width?: number;
  height?: number;
  nodes: SceneNode[];
  edges?: SceneEdge[];
  tables?: SceneTable[];
  logs?: SceneLog[];
}

export interface Row {
  cells: string[];
  tone?: Tone;
}

export interface MessageAnim {
  /** Edge to travel along. */
  edge: string;
  label: string;
  tone?: Tone;
  /** Stagger index, in units of 0.35 x base duration. */
  delay?: number;
  /** 'arrive' fades at target (default), 'reject' stops at target with a cross, 'stop' halts mid-edge. */
  outcome?: 'arrive' | 'reject' | 'stop';
}

export interface LogState {
  committed?: number;
  /** Overrides the default "committed=N" label. */
  committedLabel?: string;
  marks?: Record<number, Tone>;
}

export interface NodeState {
  tone?: Tone;
  badge?: string;
}

export interface Step {
  caption: string;
  messages?: MessageAnim[];
  /** Full row list per table; inherited from previous step when omitted. */
  tables?: Record<string, Row[]>;
  logs?: Record<string, LogState>;
  nodes?: Record<string, NodeState>;
  edges?: Record<string, { tone?: Tone; hidden?: boolean }>;
}

export interface ResolvedStep extends Required<Omit<Step, 'messages'>> {
  messages: MessageAnim[];
}

export interface Animation {
  scene: Scene;
  steps: Step[];
}

export type CalloutKind = 'misconception' | 'rule' | 'tip' | 'warning';

export interface Callout {
  kind: CalloutKind;
  title: string;
  text: string;
}

export interface CodeSample {
  lang: 'java' | 'sql' | 'yaml' | 'text' | 'bash';
  src: string;
  caption?: string;
}

export type ChartKey =
  | 'insertTps'
  | 'indexImpact'
  | 'regionLatency'
  | 'processingLatency';

export interface Slide {
  id: string;
  title: string;
  subtitle?: string;
  layout?: 'title' | 'standard' | 'section';
  bullets?: string[];
  callout?: Callout;
  code?: CodeSample;
  animation?: Animation;
  chart?: ChartKey;
  table?: { head: string[]; rows: string[][] };
  notes: string;
}

export interface Chapter {
  id: string;
  title: string;
  slides: Slide[];
}
