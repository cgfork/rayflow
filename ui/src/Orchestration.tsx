import { useEffect, useMemo, useRef, useState } from 'react';
import { Activity, ArrowLeft, Box, CalendarClock, CalendarPlus, Check, Code2, Copy, Download, GitBranch, LayoutGrid, Link2, Pencil, Plus, Rocket, Save, Star, Trash2, UserPen, UserPlus, Workflow, X } from 'lucide-react';
import { api, dateTime, send, usePaged } from './api';
import { useAuditActorName } from './audit';
import { Badge, Empty, ErrorBanner, Loading, Modal, PageHeader, Pagination, Refresh, SearchBox, UuidText } from './components';
import { DockerConfigInput, parseDockerConfig } from './DockerConfigInput';
import FlowCanvas from './FlowCanvas';
import NodeOverrides from './NodeOverrides';
import { createGraph, createNode, disconnectEdges, isJsonObject, normalizeGraph, outputChoices, removeNodes, updateGraphNode, upstreamIds, validateGraph } from './graph';
import { copyUuid } from './uuid';
import type { User } from './Auth';
import type { Graph, GraphNode } from './types';

type WorkflowTab = 'drafts' | 'published';
type WorkflowSort = 'default' | 'name-asc' | 'name-desc';

const favoritesKey = 'rayflow.workflow-favorites';

function loadFavorites(): Set<string> {
  try {
    const stored: unknown = JSON.parse(localStorage.getItem(favoritesKey) ?? '[]');
    if (Array.isArray(stored)) return new Set(stored.filter((id): id is string => typeof id === 'string'));
  } catch {
    // Favorites still work in this session when storage is unavailable.
  }
  return new Set();
}

export default function Orchestration({ actor, user, notify, dirtyChanged, openRuntime }: {
  actor: string;
  user: User | null;
  notify: (message: string) => void;
  dirtyChanged: (dirty: boolean) => void;
  openRuntime: (graphId: string) => void;
}) {
  const [tab, setTab] = useState<WorkflowTab>('drafts');
  const [search, setSearch] = useState('');
  const [sort, setSort] = useState<WorkflowSort>('default');
  const [favorites, setFavorites] = useState(loadFavorites);
  const [favoritesOnly, setFavoritesOnly] = useState(false);
  const [versions, setVersions] = useState<Pick<Graph, 'id' | 'name'> | null>(null);
  const [manualCopyId, setManualCopyId] = useState<string | null>(null);
  const manualCopyInput = useRef<HTMLInputElement>(null);
  const [editor, setEditor] = useState<{ graph: Graph; isNew: boolean } | null>(null);
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState('');
  const [opening, setOpening] = useState(false);
  const [error, setError] = useState<string>();
  const path = tab === 'drafts' ? '/graph_drafts' : versions ? `/graphs?graph_id=${encodeURIComponent(versions.id)}` : '/graphs';
  const result = usePaged<Graph>(path);
  const auditActorName = useAuditActorName(result.data?.items, user);
  const query = search.trim().toLowerCase();
  const filterFavorites = tab === 'drafts' && favoritesOnly;
  const rows = result.data?.items.filter((row) => (
    (!filterFavorites || favorites.has(row.id)) && `${row.name} ${row.id}`.toLowerCase().includes(query)
  )) ?? [];
  if (sort !== 'default') {
    const direction = sort === 'name-asc' ? 1 : -1;
    rows.sort((a, b) => direction * a.name.localeCompare(b.name, undefined, { sensitivity: 'base' }));
  }
  useEffect(() => {
    if (!manualCopyId) return;
    manualCopyInput.current?.focus();
    manualCopyInput.current?.select();
  }, [manualCopyId]);

  function toggleFavorite(id: string) {
    const next = new Set(favorites);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    setFavorites(next);
    try {
      localStorage.setItem(favoritesKey, JSON.stringify([...next]));
    } catch {
      // Keep the change in this session when storage is unavailable.
    }
  }

  function selectTab(next: WorkflowTab) {
    if (next === tab && !versions) return;
    setTab(next);
    setVersions(null);
    setError(undefined);
  }

  function showVersions(graph: Graph) {
    setVersions({ id: graph.id, name: graph.name });
    setTab('published');
    setSearch('');
    setError(undefined);
  }

  async function copy(id: string) {
    if (await copyUuid(id)) notify('Workflow ID copied.');
    else setManualCopyId(id);
  }

  function newWorkflow() {
    setName('');
    setCreating(true);
  }

  async function open(row: Graph) {
    setOpening(true); setError(undefined);
    const published = tab === 'published';
    try {
      const graph = await api<Graph>(published ? `/graphs/${row.id}?version=${row.version}` : `/graphs/${row.id}/draft`);
      setEditor({ graph, isNew: published });
    } catch (failure) { setError((failure as Error).message); }
    finally { setOpening(false); }
  }
  if (editor) return <Editor key={editor.graph.id} initial={editor.graph} isNew={editor.isNew} actor={actor} notify={notify} dirtyChanged={dirtyChanged} close={() => { setEditor(null); result.refresh(); }} />;
  return <>
    <PageHeader eyebrow="DESIGN & DEPLOY" title="Orchestration" description="Create drafts and publish workflow versions.">
      <button className="button primary" onClick={newWorkflow}><Plus size={17} />New workflow</button>
    </PageHeader>
    <ErrorBanner error={error} />
    <ErrorBanner error={result.error} retry={result.refresh} />
    <section className="workflow-list" aria-label="Workflow browser">
      <div className="workflow-toolbar">
        <div className="tabs" role="group" aria-label="Workflow status">
          <button className={tab === 'drafts' ? 'active' : ''} aria-pressed={tab === 'drafts'} disabled={opening} onClick={() => selectTab('drafts')}>Drafts</button>
          <button className={tab === 'published' ? 'active' : ''} aria-pressed={tab === 'published'} disabled={opening} onClick={() => selectTab('published')}>Published versions</button>
        </div>
        <div className="workflow-filters">
          <SearchBox value={search} onChange={setSearch} />
          {tab === 'drafts' && <button className="button workflow-favorites-filter" aria-label="My favorites on this page" aria-pressed={favoritesOnly} title="Show favorites on this page" onClick={() => setFavoritesOnly(!favoritesOnly)}><Star size={15} />My favorites</button>}
          <label className="workflow-sort">Sort this page
            <select aria-label="Sort workflows on this page" value={sort} onChange={(event) => setSort(event.target.value as WorkflowSort)}>
              <option value="default">Default order</option>
              <option value="name-asc">Name A–Z</option>
              <option value="name-desc">Name Z–A</option>
            </select>
          </label>
          <Refresh loading={result.loading} onClick={result.refresh} />
        </div>
      </div>
      {versions && <div className="workflow-version-filter">
        <span className="filter-chip"><GitBranch size={14} />Versions of <strong>{versions.name}</strong></span>
        <button type="button" className="button" disabled={opening} aria-label="Clear workflow filter" onClick={() => setVersions(null)}><X size={14} />All workflows</button>
      </div>}
      {!result.data ? (!result.error && <Loading />) : rows.length > 0 ? (
        tab === 'drafts' ? (
          <ul className="workflow-grid" aria-label="Workflows" role="list">
            {rows.map((row) => <WorkflowCard key={row.id} graph={row} favorite={favorites.has(row.id)} opening={opening} auditActorName={auditActorName}
              open={() => void open(row)} copy={() => void copy(row.id)} toggleFavorite={() => toggleFavorite(row.id)} showVersions={() => showVersions(row)} openRuntime={() => openRuntime(row.id)} />)}
          </ul>
        ) : (
          <PublishedVersions rows={rows} opening={opening} auditActorName={auditActorName} edit={(row) => void open(row)} copy={(id) => void copy(id)} openRuntime={openRuntime} />
        )
      ) : query ? (
        <Empty title="No matching workflows">Try a different name or workflow ID, or move to another page.</Empty>
      ) : filterFavorites ? (
        <Empty title="No favorite workflows on this page" action={<button className="button" onClick={() => setFavoritesOnly(false)}>Show all on this page</button>}>Select the star on a workflow to add it to your favorites.</Empty>
      ) : tab === 'drafts' ? (
        <Empty title="Your next workflow starts here" action={<button className="button primary" onClick={newWorkflow}><Plus size={16} />Create a workflow</button>}>Create a draft, add tasks, and connect them on the canvas.</Empty>
      ) : (
        <Empty title={versions ? 'No published versions for this workflow' : 'No published workflows yet'}>Open a draft and publish it to make it available to the runtime.</Empty>
      )}
      <Pagination page={result.page} count={rows.length} more={result.data?.next_cursor != null} previous={result.previous} next={result.next} />
    </section>
    {creating && <Modal title="Create a workflow" close={() => setCreating(false)}><form onSubmit={(event) => { event.preventDefault(); if (!name.trim()) return; setEditor({ graph: createGraph(name.trim()), isNew: true }); setCreating(false); }}><p className="muted">Start with a root node and build out your workflow on the canvas.</p><label className="field">Workflow name<input autoFocus required maxLength={255} value={name} placeholder="e.g. daily-data-pipeline" onChange={(event) => setName(event.target.value)} /></label><div className="modal-actions"><button type="button" className="button" onClick={() => setCreating(false)}>Cancel</button><button className="button primary" type="submit"><Plus size={16} />Create workflow</button></div></form></Modal>}
    {manualCopyId && <Modal title="Copy workflow ID" close={() => setManualCopyId(null)}>
      <p>Clipboard access is unavailable. Copy the selected workflow ID manually.</p>
      <input ref={manualCopyInput} className="mono" aria-label="Workflow ID" value={manualCopyId} readOnly />
      <div className="modal-actions"><button className="button" onClick={() => setManualCopyId(null)}>Done</button></div>
    </Modal>}
  </>;
}

function WorkflowId({ id, copy }: { id: string; copy: () => void }) {
  return <div className="workflow-id"><UuidText id={id} /><button type="button" className="icon-button" aria-label="Copy workflow ID" title="Copy workflow ID" onClick={copy}><Copy size={14} /></button></div>;
}

const auditLabels = {
  created_by: { label: 'Created by', Icon: UserPlus },
  created_at: { label: 'Created at', Icon: CalendarPlus },
  updated_by: { label: 'Updated by', Icon: UserPen },
  updated_at: { label: 'Updated at', Icon: CalendarClock },
} as const;

function AuditLabel({ field }: { field: keyof typeof auditLabels }) {
  const { label, Icon } = auditLabels[field];
  return <span className="workflow-audit-label" title={label}><Icon size={16} aria-hidden="true" /><span className="sr-only">{label}</span></span>;
}

function AuditTimestamp({ value }: { value?: string }) {
  return value ? <time dateTime={value}>{dateTime(value)}</time> : '—';
}

function AuditActor({ id, name }: { id?: string; name: string }) {
  const tooltip = id && id !== name ? `${name} (${id})` : name;
  return <dd className="workflow-audit-actor" title={tooltip}>{name}</dd>;
}

function WorkflowCard({ graph, favorite, opening, auditActorName, open, copy, toggleFavorite, showVersions, openRuntime }: {
  graph: Graph;
  favorite: boolean;
  opening: boolean;
  auditActorName: (actor?: string) => string;
  open: () => void;
  copy: () => void;
  toggleFavorite: () => void;
  showVersions: () => void;
  openRuntime: () => void;
}) {
  const connectionCount = graph.edges.length;
  return <li className="workflow-card">
    <div className="workflow-card-header">
      <span className="workflow-card-icon"><Workflow size={23} /></span>
      <div className="workflow-card-identity">
        <h2><button type="button" className="workflow-card-title" title={graph.name} disabled={opening} onClick={open}>{graph.name}</button></h2>
        <WorkflowId id={graph.id} copy={copy} />
      </div>
      <button type="button" className="icon-button workflow-card-favorite" aria-label="Favorite workflow" aria-pressed={favorite} title={favorite ? 'Remove from favorites' : 'Add to favorites'} onClick={toggleFavorite}><Star size={18} /></button>
    </div>
    <div className="workflow-card-body">
      <div className="workflow-card-meta">
        <span><Box size={15} />{graph.node_count} {graph.node_count === 1 ? 'node' : 'nodes'}</span>
        <span><Link2 size={15} />{connectionCount} {connectionCount === 1 ? 'connection' : 'connections'}</span>
      </div>
      <dl className="workflow-audit">
        <div><dt><AuditLabel field="created_by" /></dt><AuditActor id={graph.created_by} name={auditActorName(graph.created_by)} /></div>
        <div><dt><AuditLabel field="created_at" /></dt><dd><AuditTimestamp value={graph.created_at} /></dd></div>
        <div><dt><AuditLabel field="updated_by" /></dt><AuditActor id={graph.updated_by} name={auditActorName(graph.updated_by)} /></div>
        <div><dt><AuditLabel field="updated_at" /></dt><dd><AuditTimestamp value={graph.updated_at} /></dd></div>
      </dl>
    </div>
    <div className="workflow-card-footer">
      <button type="button" className="button workflow-editor-button" disabled={opening} onClick={open}><Pencil size={14} />Edit</button>
      <button type="button" className="button" disabled={opening} onClick={showVersions}>Versions</button>
      <button type="button" className="button" disabled={opening} onClick={openRuntime}><Activity size={14} />Runtime</button>
    </div>
  </li>;
}

function PublishedVersions({ rows, opening, auditActorName, edit, copy, openRuntime }: {
  rows: Graph[];
  opening: boolean;
  auditActorName: (actor?: string) => string;
  edit: (graph: Graph) => void;
  copy: (id: string) => void;
  openRuntime: (graphId: string) => void;
}) {
  return <div className="panel workflow-versions-table">
    <div className="table-scroll" role="region" aria-label="Published workflow records" tabIndex={0}>
      <table aria-label="Published versions">
        <thead>
          <tr>
            <th scope="col">Workflow</th>
            <th scope="col">Version</th>
            <th scope="col">Nodes</th>
            <th scope="col">Connections</th>
            <th scope="col"><AuditLabel field="created_by" /></th>
            <th scope="col"><AuditLabel field="created_at" /></th>
            <th scope="col"><AuditLabel field="updated_by" /></th>
            <th scope="col"><AuditLabel field="updated_at" /></th>
            <th scope="col">Actions</th>
          </tr>
        </thead>
        <tbody>{rows.map((graph) => <tr key={`${graph.id}:${graph.version}`}>
          <td>
            <button type="button" className="table-name" title={graph.name} disabled={opening} onClick={() => edit(graph)}><strong>{graph.name}</strong></button>
            <WorkflowId id={graph.id} copy={() => copy(graph.id)} />
          </td>
          <td><Badge status="published">v{graph.version}</Badge></td>
          <td>{graph.node_count} {graph.node_count === 1 ? 'node' : 'nodes'}</td>
          <td>{graph.edges.length} {graph.edges.length === 1 ? 'connection' : 'connections'}</td>
          <td className="workflow-audit-cell" title={graph.created_by}>{auditActorName(graph.created_by)}</td>
          <td className="workflow-audit-cell"><AuditTimestamp value={graph.created_at} /></td>
          <td className="workflow-audit-cell" title={graph.updated_by}>{auditActorName(graph.updated_by)}</td>
          <td className="workflow-audit-cell"><AuditTimestamp value={graph.updated_at} /></td>
          <td><div className="actions">
            <button type="button" className="button small workflow-editor-button" disabled={opening} onClick={() => edit(graph)}><Pencil size={14} />Edit</button>
            <button type="button" className="button small" disabled={opening} onClick={() => openRuntime(graph.id)}><Activity size={14} />Runtime</button>
          </div></td>
        </tr>)}</tbody>
      </table>
    </div>
  </div>;
}

function Editor({ initial, isNew, actor, notify, dirtyChanged, close }: { initial: Graph; isNew: boolean; actor: string; notify: (message: string) => void; dirtyChanged: (dirty: boolean) => void; close: () => void }) {
  const [graph, setGraph] = useState(() => normalizeGraph(initial));
  const [saved, setSaved] = useState(isNew ? '' : JSON.stringify(normalizeGraph(initial)));
  const [selected, setSelected] = useState<string | null>(initial.nodes[0]?.id ?? null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const [layout, setLayout] = useState(0);
  const [json, setJson] = useState<string | null>(null);
  const [publishing, setPublishing] = useState(false);
  const dirty = JSON.stringify(graph) !== saved;
  const validation = useMemo(() => validateGraph(graph), [graph]);
  const node = graph.nodes.find((item) => item.id === selected);
  useEffect(() => { dirtyChanged(dirty); return () => dirtyChanged(false); }, [dirty, dirtyChanged]);
  useEffect(() => {
    function unload(event: BeforeUnloadEvent) { if (dirty) { event.preventDefault(); event.returnValue = ''; } }
    window.addEventListener('beforeunload', unload);
    return () => window.removeEventListener('beforeunload', unload);
  }, [dirty]);
  async function save(publish = false) {
    setBusy(true); setError(undefined);
    try {
      const savedGraph = await send<Graph>(`/graphs/${graph.id}/draft`, { graph, actor }, 'PUT');
      setGraph(savedGraph);
      setSaved(JSON.stringify(savedGraph));
      if (publish) {
        const published = await send<Graph>(`/graphs/${graph.id}/publish`, { actor });
        notify(`Published ${graph.name} · v${published.version}.`);
        setPublishing(false);
      } else notify('Draft saved.');
    } catch (failure) { setError((failure as Error).message); setPublishing(false); }
    finally { setBusy(false); }
  }
  function updateNode(updated: GraphNode) {
    setGraph(updateGraphNode(graph, updated));
  }
  function addNode() {
    let number = graph.nodes.length + 1;
    while (graph.nodes.some((item) => item.name === `task_${number}`)) number++;
    const added = createNode(`task_${number}`);
    setGraph({ ...graph, nodes: [...graph.nodes, added], node_count: graph.nodes.length + 1 }); setSelected(added.id);
  }
  return <div className="studio">
    <div className="studio-heading"><div className="actions"><button className="icon-button" aria-label="Back to workflows" onClick={() => { if (!dirty || window.confirm('Discard unsaved workflow changes?')) close(); }}><ArrowLeft size={19} /></button><span className="table-icon"><Workflow size={20} /></span><div><input aria-label="Workflow name" className="title-input" maxLength={255} value={graph.name} disabled={busy} onChange={(event) => setGraph({ ...graph, name: event.target.value })} /><div className="studio-subtitle"><Badge status="draft">Draft</Badge><span>{dirty ? 'Unsaved changes' : 'All changes saved'}</span></div></div></div><div className="actions"><button className="button" disabled={busy} onClick={() => void save()}><Save size={15} />Save draft</button><button className="button primary" disabled={busy || validation.errors.length > 0} onClick={() => setPublishing(true)}><Rocket size={15} />Publish</button></div></div>
    <ErrorBanner error={error} />
    <div className="studio-body"><div className="canvas-panel"><div className="canvas-toolbar"><span><GitBranch size={15} />Workflow canvas <small>{graph.nodes.length} nodes · {graph.edges.length} connections</small></span><div className="actions"><button className="icon-button" title="Auto layout" aria-label="Auto layout" disabled={busy} onClick={() => setLayout((value) => value + 1)}><LayoutGrid size={17} /></button><button className="icon-button" title="Edit JSON" aria-label="Edit JSON" disabled={busy} onClick={() => setJson(JSON.stringify(graph, null, 2))}><Code2 size={18} /></button><button className="button small" disabled={busy} onClick={addNode}><Plus size={15} />Add node</button></div></div>
      <FlowCanvas graph={graph} change={busy ? undefined : setGraph} select={setSelected} selected={selected} layout={layout} />
      <div className="canvas-caption"><span><span className="status-dot" />Draft workspace</span><span>Drag to arrange · Connect handles to add dependencies</span></div>
    </div><aside className="inspector">{node ? <NodeInspector key={node.id} node={node} graph={graph} update={updateNode} change={setGraph} disabled={busy} remove={() => { setGraph(removeNodes(graph, [node.id])); setSelected(null); }} /> : <div className="inspector-empty"><Box size={28} /><h3>Select a node</h3><p>Inspect its properties, inputs, and outputs.</p></div>}</aside></div>
    <div className={`validation-panel ${validation.errors.length ? 'has-errors' : ''}`}><div><span className="validation-icon">{validation.errors.length ? <GitBranch size={18} /> : <Check size={18} />}</span><div><strong>{validation.errors.length ? 'A few things to connect' : 'Ready to publish'}</strong>{validation.errors.length ? <ul>{validation.errors.map((message) => <li key={message}>{message}</li>)}</ul> : <p>One root node. No cycles. Your workflow is ready.</p>}{validation.unused.length > 0 && <p className="muted">Unreferenced outputs (allowed): {validation.unused.join(', ')}</p>}</div></div><UuidText className="mono" id={graph.id} /></div>
    {publishing && <Modal title="Publish workflow" close={() => { if (!busy) setPublishing(false); }}><p>Save this draft and publish <strong>{graph.name}</strong> as a new immutable version?</p><p className="muted">Existing runs keep their pinned version. The server assigns the next version after validation.</p><div className="modal-actions"><button className="button" disabled={busy} onClick={() => setPublishing(false)}>Cancel</button><button className="button primary" disabled={busy} onClick={() => void save(true)}><Rocket size={15} />{busy ? 'Publishing…' : 'Publish version'}</button></div></Modal>}
    {json !== null && <JsonEditor value={json} close={() => setJson(null)} apply={(value) => { setGraph(value); setJson(null); setSelected(value.nodes[0]?.id ?? null); }} graphId={graph.id} />}
  </div>;
}

function NodeInspector({ node, graph, update, change, remove, disabled }: { node: GraphNode; graph: Graph; update: (node: GraphNode) => void; change: (graph: Graph) => void; remove: () => void; disabled: boolean }) {
  const [target, setTarget] = useState('');
  const [configuring, setConfiguring] = useState(false);
  const choices = outputChoices(graph, node.id);
  const upstream = upstreamIds(graph, node.id);
  function selectOutput(index: number, source: string) {
    const choice = choices.find((item) => item.source === source);
    const updated = { ...node, inputs: node.inputs.map((input, offset) => offset === index ? { ...input, source, file_name: input.file_name || choice?.output.file_name || '' } : input) };
    const connected = updateGraphNode(graph, updated);
    if (choice && !upstream.has(choice.producer.id)) connected.edges = [...connected.edges, [choice.producer.id, node.id]];
    change(connected);
  }
  useEffect(() => setTarget(''), [node.id]);
  return <><div className="inspector-heading"><span>NODE PROPERTIES</span><Box size={16} /></div><fieldset disabled={disabled} className="inspector-fields"><label className="field">Name<input aria-label="Node name" value={node.name} onChange={(event) => update({ ...node, name: event.target.value })} /></label><label className="field">Task type<select aria-label="Task type" value={node.node_type === 'docker' ? 'docker' : ''} onChange={(event) => update({ ...node, node_type: event.target.value })}>
      {node.node_type !== 'docker' && <option value="" disabled>Select task type</option>}
      <option value="docker">Docker</option>
    </select></label>
    <div className="field-section node-settings">
      <h3>Node settings <button type="button" className="icon-button" title="Edit Docker configuration JSON" aria-label="Edit Docker configuration JSON" onClick={() => setConfiguring(true)}><Code2 size={15} /></button></h3>
      <label className="field">Docker image<input aria-label="Docker image" placeholder="alpine:3.21" value={typeof node.settings.config?.image === 'string' ? node.settings.config.image : ''} onChange={(event) => update({ ...node, settings: { ...node.settings, config: { ...node.settings.config, image: event.target.value } } })} /></label>
      <div className="settings-resources">
        <label className="field">CPU cores<input aria-label="CPU cores" type="number" min="1" max="4294967295" step="1" value={node.settings.resource.cpu_cores} onChange={(event) => update({ ...node, settings: { ...node.settings, resource: { ...node.settings.resource, cpu_cores: Number(event.target.value) } } })} /></label>
        <label className="field">Memory (MiB)<input aria-label="Memory (MiB)" type="number" min="1" step="1" value={node.settings.resource.memory_bytes / 1048576} onChange={(event) => update({ ...node, settings: { ...node.settings, resource: { ...node.settings.resource, memory_bytes: Math.round(Number(event.target.value) * 1048576) } } })} /></label>
      </div>
      <label className="field">Timeout<input aria-label="Timeout" placeholder="10s" value={node.settings.timeout} onChange={(event) => update({ ...node, settings: { ...node.settings, timeout: event.target.value } })} /></label>
    </div>
    <NodeOverrides settings={node.settings} update={(overrides) => update({ ...node, settings: { ...node.settings, overrides: Object.keys(overrides).length ? overrides : undefined } })} />
    <div className="field-section"><h3>Inputs <button type="button" className="icon-button" aria-label="Add input" onClick={() => update({ ...node, inputs: [...node.inputs, { file_name: '', source: '', required: true, format: null }] })}><Plus size={15} /></button></h3>{node.inputs.length === 0 && <p className="muted small-text">No inputs.</p>}{node.inputs.map((input, index) => <div className="port-form" key={index}><div className="actions"><input aria-label={`Input ${index + 1} file name`} placeholder="File name" value={input.file_name} onChange={(event) => update({ ...node, inputs: node.inputs.map((item, offset) => offset === index ? { ...item, file_name: event.target.value } : item) })} /><button type="button" className="icon-button" aria-label={`Remove input ${index + 1}`} onClick={() => update({ ...node, inputs: node.inputs.filter((_, offset) => offset !== index) })}><X size={14} /></button></div><select aria-label={`Input ${index + 1} source`} value={input.source} onChange={(event) => selectOutput(index, event.target.value)}><option value="">Select output</option>{input.source && !choices.some((choice) => choice.source === input.source) && <option value={input.source} disabled>{input.source} (unavailable)</option>}{choices.map((choice) => <option key={choice.source} value={choice.source}>{choice.source}</option>)}</select><input aria-label={`Input ${index + 1} format`} placeholder="Format (optional)" value={input.format ?? ''} onChange={(event) => update({ ...node, inputs: node.inputs.map((item, offset) => offset === index ? { ...item, format: event.target.value || null } : item) })} /><label className="checkbox"><input type="checkbox" checked={input.required} onChange={(event) => update({ ...node, inputs: node.inputs.map((item, offset) => offset === index ? { ...item, required: event.target.checked } : item) })} />Required</label></div>)}</div>
    <div className="field-section"><h3>Outputs <button type="button" className="icon-button" aria-label="Add output" onClick={() => update({ ...node, outputs: [...node.outputs, { file_name: '', kind: 'File', multiple: false }] })}><Plus size={15} /></button></h3>{node.outputs.length === 0 && <p className="muted small-text">No outputs defined.</p>}{node.outputs.map((output, index) => <div className="port-form" key={index}><div className="actions"><input aria-label={`Output ${index + 1} file name`} placeholder="File name" value={output.file_name} onChange={(event) => update({ ...node, outputs: node.outputs.map((item, offset) => offset === index ? { ...item, file_name: event.target.value } : item) })} /><button type="button" className="icon-button" aria-label={`Remove output ${index + 1}`} onClick={() => update({ ...node, outputs: node.outputs.filter((_, offset) => offset !== index) })}><X size={14} /></button></div><select aria-label={`Output ${index + 1} kind`} value={output.kind} onChange={(event) => update({ ...node, outputs: node.outputs.map((item, offset) => offset === index ? { ...item, kind: event.target.value as 'File' | 'Directory' } : item) })}><option>File</option><option>Directory</option></select><label className="checkbox"><input type="checkbox" checked={output.multiple} onChange={(event) => update({ ...node, outputs: node.outputs.map((item, offset) => offset === index ? { ...item, multiple: event.target.checked } : item) })} />Multiple files</label></div>)}</div>
    <div className="field-section"><h3>Connections <Link2 size={14} /></h3>{graph.edges.filter(([source]) => source === node.id).map(([source, destination]) => <div className="connection" key={destination}><span>→ {graph.nodes.find((item) => item.id === destination)?.name}</span><button type="button" className="icon-button" aria-label="Remove connection" onClick={() => change(disconnectEdges(graph, [[source, destination]]))}><X size={13} /></button></div>)}<div className="actions"><select aria-label="Connect to node" value={target} onChange={(event) => setTarget(event.target.value)}><option value="">Connect to…</option>{graph.nodes.filter((item) => item.id !== node.id && !upstream.has(item.id) && !graph.edges.some(([source, destination]) => source === node.id && destination === item.id)).map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}</select><button type="button" className="icon-button" disabled={!target} aria-label="Add connection" onClick={() => { change({ ...graph, edges: [...graph.edges, [node.id, target]] }); setTarget(''); }}><Plus size={16} /></button></div></div>
    <button type="button" className="button danger subtle" onClick={remove}><Trash2 size={15} />Delete node</button><UuidText className="mono inspector-id" id={node.id} />
  </fieldset>{configuring && <DockerConfigEditor config={node.settings.config} close={() => setConfiguring(false)} apply={(config) => { update({ ...node, settings: { ...node.settings, config } }); setConfiguring(false); }} />}</>;
}

function DockerConfigEditor({ config, close, apply }: { config: GraphNode['settings']['config']; close: () => void; apply: (config: GraphNode['settings']['config']) => void }) {
  const initialValue = JSON.stringify(config ?? {}, null, 2);
  const [text, setText] = useState(initialValue);
  const [error, setError] = useState<string>();
  function submit() {
    try {
      apply(parseDockerConfig(text));
    } catch (failure) { setError((failure as Error).message); }
  }
  return <Modal title="Docker configuration" close={close} wide>
    <ErrorBanner error={error} />
    <DockerConfigInput initialValue={initialValue} onChange={setText} />
    <div className="modal-actions"><button className="button" onClick={close}>Cancel</button><button className="button primary" onClick={submit}><Check size={15} />Apply configuration</button></div>
  </Modal>;
}

function isDraftResource(value: unknown): boolean {
  if (!isJsonObject(value)) return false;
  const { cpu_cores, memory_bytes } = value;
  return typeof cpu_cores === 'number' && Number.isInteger(cpu_cores) && cpu_cores >= 0 && cpu_cores <= 4294967295
    && typeof memory_bytes === 'number' && Number.isSafeInteger(memory_bytes) && memory_bytes >= 0;
}

function isDraftOverride(value: unknown): boolean {
  return isJsonObject(value)
    && (value.annotation_match == null || typeof value.annotation_match === 'string')
    && (value.config == null || isJsonObject(value.config))
    && (value.resource == null || isDraftResource(value.resource))
    && (value.timeout == null || typeof value.timeout === 'string');
}

function JsonEditor({ value, close, apply, graphId }: { value: string; close: () => void; apply: (graph: Graph) => void; graphId: string }) {
  const [text, setText] = useState(value);
  const [error, setError] = useState<string>();
  function submit() {
    try {
      const graph = JSON.parse(text) as Graph;
      const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
      if (graph.id !== graphId || typeof graph.name !== 'string' || !Number.isSafeInteger(graph.version) || graph.version < 0 || !Array.isArray(graph.nodes) || !Array.isArray(graph.edges)) throw new Error('Keep the workflow ID and provide a name, nonnegative version, nodes, and edges.');
      for (const node of graph.nodes) {
        if (!uuid.test(node.id) || typeof node.name !== 'string' || typeof node.node_type !== 'string' || !Array.isArray(node.inputs) || !Array.isArray(node.outputs)) throw new Error('Each node needs a UUID, name, node_type, inputs, and outputs.');
        if (node.settings !== undefined) {
          const settings = node.settings;
          if (!settings || !isJsonObject(settings.config) || !isDraftResource(settings.resource) || typeof settings.timeout !== 'string') throw new Error('Node settings need a JSON config object, resource.cpu_cores, resource.memory_bytes, and a duration-string timeout.');
          if (settings.overrides !== undefined && (!isJsonObject(settings.overrides) || !Object.values(settings.overrides).every(isDraftOverride))) throw new Error('Node overrides must be an object keyed by override name. Each override can specify annotation_match, config, resource, and timeout.');
        }
        if (node.inputs.some((input) => !input || typeof input.file_name !== 'string' || typeof input.source !== 'string' || typeof input.required !== 'boolean' || !(input.format === null || typeof input.format === 'string'))) throw new Error('Inputs need file_name, source, required, and format (string or null).');
        if (node.outputs.some((output) => !output || typeof output.file_name !== 'string' || !['File', 'Directory'].includes(output.kind) || typeof output.multiple !== 'boolean')) throw new Error('Outputs need file_name, kind (File or Directory), and multiple.');
      }
      if (graph.edges.some((edge) => !Array.isArray(edge) || edge.length !== 2 || !edge.every((id) => uuid.test(id)))) throw new Error('Edges must be pairs of node UUIDs.');
      apply(normalizeGraph(graph));
    } catch (failure) { setError((failure as Error).message); }
  }
  function download() {
    const url = URL.createObjectURL(new Blob([text], { type: 'application/json' }));
    const anchor = document.createElement('a'); anchor.href = url; anchor.download = `workflow-${graphId.slice(0, 8)}.json`; anchor.click(); URL.revokeObjectURL(url);
  }
  return <Modal title="Workflow JSON" close={close} wide><p className="muted">Edit the graph definition, or export it for version control.</p><ErrorBanner error={error} /><textarea className="json-editor" spellCheck={false} aria-label="Workflow JSON" value={text} onChange={(event) => setText(event.target.value)} /><div className="modal-actions"><button className="button" onClick={download}><Download size={15} />Export JSON</button><button className="button primary" onClick={submit}>Apply changes</button></div></Modal>;
}
