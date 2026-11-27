import { useMemo, useState } from 'react';
import { GitCompare } from 'lucide-react';
import { shortId, usePaged, useRemote } from './api';
import { Badge, ErrorBanner, Loading, Modal, Pagination } from './components';
import FlowCanvas, { type CanvasChange, type CanvasDiff } from './FlowCanvas';
import type { AnnotationMatchNodeSettings, Graph } from './types';

type GraphVersion = Pick<Graph, 'id' | 'name' | 'version'>;
type Scalar = string | number | boolean | null;
type DiffValue = Scalar | { port: Record<string, Scalar> } | { json: unknown };
type FieldDiff<Field = string, Value = Scalar> = { field: Field; change: [Value, Value] };
type PortDiff =
  | { kind: 'changed'; index: number; changes: FieldDiff[] }
  | { kind: 'added' | 'removed'; index: number; value: Record<string, Scalar> };
type OverrideDiff = { name: string; before: AnnotationMatchNodeSettings | null; after: AnnotationMatchNodeSettings | null };
type SettingsDiff =
  | FieldDiff<'config', unknown>
  | FieldDiff<'timeout', string>
  | { field: 'resource'; change: FieldDiff<'cpu_cores' | 'memory_bytes' | 'gpu_memory_bytes' | 'gpu_cdi_device'>[] }
  | { field: 'overrides'; change: OverrideDiff[] };
type NodeDiff =
  | FieldDiff<'id' | 'name' | 'node_type'>
  | { field: 'settings'; change: SettingsDiff[] }
  | { field: 'inputs'; change: PortDiff[] }
  | { field: 'outputs'; change: PortDiff[] }
  | { field: 'assets'; change: PortDiff[] };
type Edge = [string, string];

interface StructureDiff {
  node_diffs: [string, NodeDiff[]][];
  node_miss: string[];
  node_over: string[];
  edge_miss: Edge[];
  edge_over: Edge[];
}

const workflowFields = {
  id: 'Workflow ID', name: 'Workflow name', version: 'Version', node_count: 'Node count',
  created_at: 'Created at', created_by: 'Created by', updated_at: 'Updated at', updated_by: 'Updated by',
};

interface GraphComparison {
  changes: (FieldDiff<keyof typeof workflowFields> | { field: 'structure'; change: StructureDiff })[];
  before_node_names: Record<string, string>;
  after_node_names: Record<string, string>;
}

const nodeFields: Record<string, string> = {
  id: 'Node ID', name: 'Name', node_type: 'Node type', file_name: 'File name',
  source: 'Source', required: 'Required', format: 'Format', kind: 'Kind',
  asset_id: 'Asset ID',
  cpu_cores: 'CPU cores', memory_bytes: 'Memory (bytes)', gpu_memory_bytes: 'GPU memory (bytes)',
  gpu_cdi_device: 'GPU CDI device',
};

function versionKey(graph: GraphVersion) {
  return `${graph.id}:${graph.version}`;
}

function diffPath(base: GraphVersion, target: GraphVersion) {
  const query = new URLSearchParams({ from_version: String(base.version), version: String(target.version) });
  if (base.id !== target.id) query.set('from_graph_id', base.id);
  return `/graphs/${encodeURIComponent(target.id)}/diff?${query}`;
}

export default function GraphDiffDialog({ target, close }: { target: Graph; close: () => void }) {
  const [allWorkflows, setAllWorkflows] = useState(false);
  const [selected, setSelected] = useState('');
  const [base, setBase] = useState<Graph>();
  const versions = usePaged<Graph>(allWorkflows ? '/graphs' : `/graphs?graph_id=${encodeURIComponent(target.id)}`);
  const choices = versions.data?.items.filter((graph) => versionKey(graph) !== versionKey(target)) ?? [];
  const chosen = choices.find((graph) => versionKey(graph) === selected);
  const comparison = useRemote<GraphComparison>(base ? diffPath(base, target) : null);

  function selectVersion(value: string) {
    setSelected(value);
    setBase(undefined);
  }

  return <Modal title="Compare graph versions" close={close} wide>
    <p className="graph-diff-target"><span className="muted">Target</span> <strong>{target.name}</strong> <Badge status="published">v{target.version}</Badge></p>
    <form onSubmit={(event) => { event.preventDefault(); if (chosen && !base) setBase(chosen); }}>
      <div className="graph-diff-controls">
        <label className="field">Base version
          <select value={selected} disabled={!choices.length || !!versions.error} onChange={(event) => selectVersion(event.target.value)}>
            <option value="">Choose a published version</option>
            {choices.map((graph) => <option key={versionKey(graph)} value={versionKey(graph)}>
              {graph.name} · v{graph.version}{allWorkflows ? ` · ${shortId(graph.id)}` : ''}
            </option>)}
          </select>
        </label>
        <button type="submit" className="button primary" disabled={!chosen || !!versions.error || !!base}><GitCompare size={15} />Compare</button>
      </div>
      <label className="checkbox"><input type="checkbox" checked={allWorkflows} onChange={(event) => {
        setAllWorkflows(event.target.checked);
        selectVersion('');
      }} />Include other workflows</label>
    </form>
    <ErrorBanner error={versions.error} retry={versions.refresh} />
    {!versions.data && !versions.error && <Loading label="Loading published versions…" />}
    {versions.data && !versions.error && !choices.length && <p className="info-banner">No other published versions on this page.</p>}
    {(versions.page > 1 || versions.data?.next_cursor != null) && <Pagination page={versions.page} count={choices.length} more={versions.data?.next_cursor != null}
      previous={() => { selectVersion(''); versions.previous(); }} next={() => { selectVersion(''); versions.next(); }} />}
    <ErrorBanner error={comparison.error} retry={comparison.refresh} />
    {base && !comparison.data && (!comparison.error || comparison.loading) && <Loading label="Comparing graph versions…" />}
    {comparison.data && base && <ComparisonResults comparison={comparison.data} before={base} after={target} />}
  </Modal>;
}

type DiffRow = { key: string; label: string; values: [DiffValue, DiffValue] };

function nodeFieldName(field: string) {
  return nodeFields[field] ?? field;
}

function settingsRows(changes: SettingsDiff[]): DiffRow[] {
  return changes.flatMap<DiffRow>(({ field, change }) => {
    switch (field) {
      case 'config':
        return [{ key: 'settings:config', label: 'Settings · Configuration', values: [{ json: change[0] }, { json: change[1] }] }];
      case 'resource':
        return change.map(({ field, change }) => ({ key: `settings:resource:${field}`, label: `Settings · ${nodeFieldName(field)}`, values: change }));
      case 'timeout':
        return [{ key: 'settings:timeout', label: 'Settings · Timeout', values: change }];
      case 'overrides':
        return change.map(({ name, before, after }) => ({
          key: `settings:override:${name}`,
          label: `Settings · Override · ${name}`,
          values: [before === null ? null : { json: before }, after === null ? null : { json: after }],
        }));
    }
  });
}

function nodeRows(changes: NodeDiff[]): DiffRow[] {
  return changes.flatMap<DiffRow>(({ field, change }) => {
    if (field === 'settings') return settingsRows(change);
    if (field === 'inputs' || field === 'outputs' || field === 'assets') {
      return change.flatMap<DiffRow>((port) => {
        const label = `${{ inputs: 'Input', outputs: 'Output', assets: 'Asset' }[field]} ${port.index + 1}`;
        if (port.kind === 'changed') {
          return port.changes.map(({ field, change }) => ({ key: `${label}:${field}`, label: `${label} · ${nodeFieldName(field)}`, values: change }));
        }
        const value = { port: port.value };
        return [{
          key: label,
          label: `${label} · ${port.kind === 'added' ? 'Added' : 'Removed'}`,
          values: port.kind === 'added' ? [null, value] : [value, null],
        }];
      });
    }
    return [{ key: field, label: nodeFieldName(field), values: change }];
  });
}

function scalarValue(value: Scalar) {
  if (value === null) return '—';
  return value === '' ? '""' : String(value);
}

function ChangeValue({ value }: { value: DiffValue }) {
  if (value === null || typeof value !== 'object') return scalarValue(value);
  if ('json' in value) return <pre className="graph-diff-json">{JSON.stringify(value.json, null, 2)}</pre>;
  return <dl className="graph-diff-value">{Object.entries(value.port).map(([field, item]) => <div key={field}>
    <dt>{nodeFieldName(field)}</dt><dd>{scalarValue(item)}</dd>
  </div>)}</dl>;
}

function ChangeTable({ label, rows }: { label: string; rows: DiffRow[] }) {
  return <table className="graph-diff-table" aria-label={label}>
    <thead><tr><th scope="col">Change</th><th scope="col">Before</th><th scope="col">After</th></tr></thead>
    <tbody>{rows.map(({ key, label, values: [before, after] }) => <tr key={key}>
      <th scope="row">{label}</th>
      <td className={before === null ? 'muted' : 'graph-diff-before'}><ChangeValue value={before} /></td>
      <td className={after === null ? 'muted' : 'graph-diff-after'}><ChangeValue value={after} /></td>
    </tr>)}</tbody>
  </table>;
}

function comparisonCanvas(before: Graph, after: Graph, structure?: StructureDiff): { graph: Graph; diff: CanvasDiff } {
  const nodeChanges = new Map<string, CanvasChange>();
  for (const id of structure?.node_miss ?? []) nodeChanges.set(id, 'added');
  for (const id of structure?.node_over ?? []) nodeChanges.set(id, 'removed');
  for (const [id] of structure?.node_diffs ?? []) nodeChanges.set(id, 'modified');

  const removedEdges = structure?.edge_over ?? [];
  const edgeChanges = new Map<string, 'added' | 'removed'>();
  for (const [source, target] of structure?.edge_miss ?? []) edgeChanges.set(`${source}:${target}`, 'added');
  for (const [source, target] of removedEdges) edgeChanges.set(`${source}:${target}`, 'removed');

  const nodes = [...after.nodes, ...before.nodes.filter((node) => nodeChanges.get(node.id) === 'removed')];
  return {
    graph: { ...after, nodes, node_count: nodes.length, edges: [...after.edges, ...removedEdges] },
    diff: { nodes: nodeChanges, edges: edgeChanges },
  };
}

function ComparisonResults({ comparison, before, after }: { comparison: GraphComparison; before: Graph; after: Graph }) {
  const [selected, setSelected] = useState<string | null>(null);
  const { changes, before_node_names: beforeNames, after_node_names: afterNames } = comparison;
  const metadata = changes.filter((change) => change.field !== 'structure');
  const structure = changes.find((change) => change.field === 'structure')?.change;
  const canvas = useMemo(() => comparisonCanvas(before, after, structure), [before, after, structure]);
  const nodes: DiffRow[] = [
    ...(structure?.node_over ?? []).map((id): DiffRow => ({ key: id, label: 'Removed', values: [beforeNames[id] ?? id, null] })),
    ...(structure?.node_miss ?? []).map((id): DiffRow => ({ key: id, label: 'Added', values: [null, afterNames[id] ?? id] })),
  ];
  const edges: DiffRow[] = [
    ...(structure?.edge_over ?? []).map(([from, to]): DiffRow => ({
      key: `${from}:${to}`, label: 'Removed', values: [`${beforeNames[from] ?? from} → ${beforeNames[to] ?? to}`, null],
    })),
    ...(structure?.edge_miss ?? []).map(([from, to]): DiffRow => ({
      key: `${from}:${to}`, label: 'Added', values: [null, `${afterNames[from] ?? from} → ${afterNames[to] ?? to}`],
    })),
  ];
  const modified = structure?.node_diffs ?? [];
  const shownNodes = selected ? nodes.filter((row) => row.key === selected) : nodes;
  const shownModified = selected ? modified.filter(([id]) => id === selected) : modified;

  return <section className="graph-diff-results" aria-label="Graph differences">
    <section className="graph-diff-canvas" aria-label="Graph comparison canvas">
      <ul className="graph-diff-legend" aria-label="Change legend">
        <li data-change="added">Added</li><li data-change="removed">Removed</li><li data-change="modified">Modified</li>
      </ul>
      <FlowCanvas graph={canvas.graph} diff={canvas.diff} selected={selected} select={setSelected} />
      <div className="canvas-caption">Select a node to inspect its changes</div>
    </section>
    {!structure && <p className="info-banner" role="status">{changes.length ? 'The nodes and connections match.' : 'The selected versions match.'}</p>}
    {(nodes.length > 0 || modified.length > 0 || selected) && <section>
      <div className="graph-diff-node-heading">
        <h3>Nodes</h3>
        {selected && <button type="button" className="button small" onClick={() => setSelected(null)}>Show all node changes</button>}
      </div>
      {selected && !canvas.diff.nodes.has(selected) && <p className="info-banner" role="status">{afterNames[selected] ?? selected} is unchanged.</p>}
      {shownNodes.length > 0 && <ChangeTable label="Added and removed nodes" rows={shownNodes} />}
      {shownModified.map(([id, changes]) => {
        const name = afterNames[id] ?? beforeNames[id] ?? id;
        return <div className="graph-diff-node" key={id}>
          <h4 title={id}>{name}</h4><ChangeTable label={`Changes to ${name}`} rows={nodeRows(changes)} />
        </div>;
      })}
    </section>}
    {edges.length > 0 && <section><h3>Connections</h3><ChangeTable label="Connection changes" rows={edges} /></section>}
    {metadata.length > 0 && <section><h3>Workflow fields</h3>
      <ChangeTable label="Workflow field changes" rows={metadata.map(({ field, change }) => ({ key: field, label: workflowFields[field], values: change }))} />
    </section>}
  </section>;
}
