import { useEffect, useMemo, useState } from 'react';
import { ArrowLeft, ArrowUpRight, Box, Check, Code2, Download, GitBranch, LayoutGrid, Link2, Plus, Rocket, Save, Trash2, Workflow, X } from 'lucide-react';
import { ApiError, api, send, shortId, usePaged } from './api';
import { Badge, Empty, ErrorBanner, Loading, Modal, PageHeader, Pagination, Refresh, SearchBox } from './components';
import FlowCanvas from './FlowCanvas';
import { createGraph, createNode, disconnectEdges, normalizeGraph, outputChoices, removeNodes, updateGraphNode, upstreamIds, validateGraph } from './graph';
import type { Graph, GraphNode } from './types';

export default function Orchestration({ actor, notify, dirtyChanged }: { actor: string; notify: (message: string) => void; dirtyChanged: (dirty: boolean) => void }) {
  const [tab, setTab] = useState<'drafts' | 'published'>('drafts');
  const [search, setSearch] = useState('');
  const [editor, setEditor] = useState<{ graph: Graph; isNew: boolean } | null>(null);
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState('');
  const [opening, setOpening] = useState(false);
  const [error, setError] = useState<string>();
  const result = usePaged<Graph>(tab === 'drafts' ? '/graph_drafts' : '/graphs');
  const rows = result.data?.items.filter((row) => `${row.name} ${row.id}`.toLowerCase().includes(search.toLowerCase())) ?? [];
  async function open(row: Graph) {
    setOpening(true); setError(undefined);
    try {
      if (tab === 'drafts') {
        const graph = await api<Graph>(`/graphs/${row.id}/draft`);
        setEditor({ graph, isNew: false });
      } else {
        try {
          const draft = await api<Graph>(`/graphs/${row.id}/draft`);
          setEditor({ graph: draft, isNew: false });
          notify('Opened the existing draft. Published versions remain immutable.');
        } catch (failure) {
          if (!(failure instanceof ApiError) || failure.status !== 404) throw failure;
          const graph = await api<Graph>(`/graphs/${row.id}?version=${row.version}`);
          setEditor({ graph, isNew: true });
        }
      }
    } catch (failure) { setError((failure as Error).message); }
    finally { setOpening(false); }
  }
  if (editor) return <Editor key={editor.graph.id} initial={editor.graph} isNew={editor.isNew} actor={actor} notify={notify} dirtyChanged={dirtyChanged} close={() => { setEditor(null); result.refresh(); }} />;
  return <>
    <PageHeader eyebrow="DESIGN & DEPLOY" title="Orchestration" description="Turn connected tasks into reliable workflows.">
      <button className="button primary" onClick={() => { setName(''); setCreating(true); }}><Plus size={17} />New workflow</button>
    </PageHeader>
    <div className="intro-strip"><div className="intro-icon"><Workflow size={27} /></div><div><strong>A clear path from idea to execution.</strong><p>Build your graph, connect its nodes, and publish an immutable version.</p></div><div className="intro-steps"><span>01 <b>Build</b></span><span>02 <b>Validate</b></span><span>03 <b>Publish</b></span></div></div>
    <ErrorBanner error={error || result.error} retry={result.refresh} />
    <section className="panel">
      <div className="panel-toolbar"><div className="tabs"><button className={tab === 'drafts' ? 'active' : ''} onClick={() => setTab('drafts')}>Drafts</button><button className={tab === 'published' ? 'active' : ''} onClick={() => setTab('published')}>Published versions</button></div><div className="actions"><SearchBox value={search} onChange={setSearch} /><Refresh loading={result.loading} onClick={result.refresh} /></div></div>
      {!result.data && result.loading ? <Loading /> : !rows.length ? <Empty title={search ? 'No matching workflows' : tab === 'drafts' ? 'Your next workflow starts here' : 'No published workflows yet'} action={!search && tab === 'drafts' ? <button className="button primary" onClick={() => setCreating(true)}><Plus size={16} />Create a workflow</button> : undefined}>{search ? 'Try a different name or workflow ID.' : tab === 'drafts' ? 'Create a draft, add tasks, and connect them on the canvas.' : 'Open a draft and publish it to make it available to the runtime.'}</Empty> : <div className="table-scroll"><table><thead><tr><th>Workflow</th><th>Version</th><th>Nodes</th><th /></tr></thead><tbody>{rows.map((row) => <tr key={`${row.id}:${row.version}`}><td><button className="table-name" disabled={opening} onClick={() => void open(row)}><span className="table-icon"><Workflow size={19} /></span><span><strong>{row.name}</strong><small>{shortId(row.id)}</small></span></button></td><td><Badge status={tab === 'drafts' ? 'draft' : 'published'}>{tab === 'drafts' ? 'Draft' : `v${row.version}`}</Badge></td><td><span className="inline-meta"><Box size={14} />{row.node_count}</span></td><td><button className="icon-button" disabled={opening} onClick={() => void open(row)} aria-label={`Edit ${row.name}`}><ArrowUpRight size={18} /></button></td></tr>)}</tbody></table></div>}
      <Pagination page={result.page} count={rows.length} more={result.data?.next_cursor != null} previous={result.previous} next={result.next} />
    </section>
    <div className="section-note"><GitBranch size={14} />Drafts are editable. Published versions are immutable and ready to run.</div>
    {creating && <Modal title="Create a workflow" close={() => setCreating(false)}><form onSubmit={(event) => { event.preventDefault(); if (!name.trim()) return; setEditor({ graph: createGraph(name.trim()), isNew: true }); setCreating(false); }}><p className="muted">Start with a root node and build out your workflow on the canvas.</p><label className="field">Workflow name<input autoFocus required maxLength={255} value={name} placeholder="e.g. daily-data-pipeline" onChange={(event) => setName(event.target.value)} /></label><div className="modal-actions"><button type="button" className="button" onClick={() => setCreating(false)}>Cancel</button><button className="button primary" type="submit"><Plus size={16} />Create workflow</button></div></form></Modal>}
  </>;
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
    <div className={`validation-panel ${validation.errors.length ? 'has-errors' : ''}`}><div><span className="validation-icon">{validation.errors.length ? <GitBranch size={18} /> : <Check size={18} />}</span><div><strong>{validation.errors.length ? 'A few things to connect' : 'Ready to publish'}</strong>{validation.errors.length ? <ul>{validation.errors.map((message) => <li key={message}>{message}</li>)}</ul> : <p>One root node. No cycles. Your workflow is ready.</p>}{validation.unused.length > 0 && <p className="muted">Unreferenced outputs (allowed): {validation.unused.join(', ')}</p>}</div></div><span className="mono">{shortId(graph.id)}</span></div>
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
    <div className="field-section"><h3>Inputs <button type="button" className="icon-button" aria-label="Add input" onClick={() => update({ ...node, inputs: [...node.inputs, { file_name: '', source: '', required: true, format: null }] })}><Plus size={15} /></button></h3>{node.inputs.length === 0 && <p className="muted small-text">No inputs.</p>}{node.inputs.map((input, index) => <div className="port-form" key={index}><div className="actions"><input aria-label={`Input ${index + 1} file name`} placeholder="File name" value={input.file_name} onChange={(event) => update({ ...node, inputs: node.inputs.map((item, offset) => offset === index ? { ...item, file_name: event.target.value } : item) })} /><button type="button" className="icon-button" aria-label={`Remove input ${index + 1}`} onClick={() => update({ ...node, inputs: node.inputs.filter((_, offset) => offset !== index) })}><X size={14} /></button></div><select aria-label={`Input ${index + 1} source`} value={input.source} onChange={(event) => selectOutput(index, event.target.value)}><option value="">Select output</option>{input.source && !choices.some((choice) => choice.source === input.source) && <option value={input.source} disabled>{input.source} (unavailable)</option>}{choices.map((choice) => <option key={choice.source} value={choice.source}>{choice.source}</option>)}</select><input aria-label={`Input ${index + 1} format`} placeholder="Format (optional)" value={input.format ?? ''} onChange={(event) => update({ ...node, inputs: node.inputs.map((item, offset) => offset === index ? { ...item, format: event.target.value || null } : item) })} /><label className="checkbox"><input type="checkbox" checked={input.required} onChange={(event) => update({ ...node, inputs: node.inputs.map((item, offset) => offset === index ? { ...item, required: event.target.checked } : item) })} />Required</label></div>)}</div>
    <div className="field-section"><h3>Outputs <button type="button" className="icon-button" aria-label="Add output" onClick={() => update({ ...node, outputs: [...node.outputs, { file_name: '', kind: 'File', multiple: false }] })}><Plus size={15} /></button></h3>{node.outputs.length === 0 && <p className="muted small-text">No outputs defined.</p>}{node.outputs.map((output, index) => <div className="port-form" key={index}><div className="actions"><input aria-label={`Output ${index + 1} file name`} placeholder="File name" value={output.file_name} onChange={(event) => update({ ...node, outputs: node.outputs.map((item, offset) => offset === index ? { ...item, file_name: event.target.value } : item) })} /><button type="button" className="icon-button" aria-label={`Remove output ${index + 1}`} onClick={() => update({ ...node, outputs: node.outputs.filter((_, offset) => offset !== index) })}><X size={14} /></button></div><select aria-label={`Output ${index + 1} kind`} value={output.kind} onChange={(event) => update({ ...node, outputs: node.outputs.map((item, offset) => offset === index ? { ...item, kind: event.target.value as 'File' | 'Directory' } : item) })}><option>File</option><option>Directory</option></select><label className="checkbox"><input type="checkbox" checked={output.multiple} onChange={(event) => update({ ...node, outputs: node.outputs.map((item, offset) => offset === index ? { ...item, multiple: event.target.checked } : item) })} />Multiple files</label></div>)}</div>
    <div className="field-section"><h3>Connections <Link2 size={14} /></h3>{graph.edges.filter(([source]) => source === node.id).map(([source, destination]) => <div className="connection" key={destination}><span>→ {graph.nodes.find((item) => item.id === destination)?.name}</span><button type="button" className="icon-button" aria-label="Remove connection" onClick={() => change(disconnectEdges(graph, [[source, destination]]))}><X size={13} /></button></div>)}<div className="actions"><select aria-label="Connect to node" value={target} onChange={(event) => setTarget(event.target.value)}><option value="">Connect to…</option>{graph.nodes.filter((item) => item.id !== node.id && !upstream.has(item.id) && !graph.edges.some(([source, destination]) => source === node.id && destination === item.id)).map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}</select><button type="button" className="icon-button" disabled={!target} aria-label="Add connection" onClick={() => { change({ ...graph, edges: [...graph.edges, [node.id, target]] }); setTarget(''); }}><Plus size={16} /></button></div></div>
    <button type="button" className="button danger subtle" onClick={remove}><Trash2 size={15} />Delete node</button><span className="mono inspector-id">{node.id}</span>
  </fieldset>{configuring && <DockerConfigEditor config={node.settings.config} close={() => setConfiguring(false)} apply={(config) => { update({ ...node, settings: { ...node.settings, config } }); setConfiguring(false); }} />}</>;
}

function DockerConfigEditor({ config, close, apply }: { config: GraphNode['settings']['config']; close: () => void; apply: (config: GraphNode['settings']['config']) => void }) {
  const [text, setText] = useState(JSON.stringify(config ?? {}, null, 2));
  const [error, setError] = useState<string>();
  function submit() {
    try {
      const value: unknown = JSON.parse(text);
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Docker configuration must be a JSON object.');
      apply(value as GraphNode['settings']['config']);
    } catch (failure) { setError((failure as Error).message); }
  }
  return <Modal title="Docker configuration" close={close} wide><ErrorBanner error={error} /><textarea className="json-editor" spellCheck={false} aria-label="Docker configuration JSON" value={text} onChange={(event) => setText(event.target.value)} /><div className="modal-actions"><button className="button" onClick={close}>Cancel</button><button className="button primary" onClick={submit}><Check size={15} />Apply configuration</button></div></Modal>;
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
          if (!settings || !settings.config || typeof settings.config !== 'object' || Array.isArray(settings.config) || !settings.resource || !Number.isInteger(settings.resource.cpu_cores) || settings.resource.cpu_cores < 0 || settings.resource.cpu_cores > 4294967295 || !Number.isSafeInteger(settings.resource.memory_bytes) || settings.resource.memory_bytes < 0 || typeof settings.timeout !== 'string') throw new Error('Node settings need a JSON config object, resource.cpu_cores, resource.memory_bytes, and a duration-string timeout.');
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
    const anchor = document.createElement('a'); anchor.href = url; anchor.download = `workflow-${shortId(graphId)}.json`; anchor.click(); URL.revokeObjectURL(url);
  }
  return <Modal title="Workflow JSON" close={close} wide><p className="muted">Edit the graph definition, or export it for version control.</p><ErrorBanner error={error} /><textarea className="json-editor" spellCheck={false} aria-label="Workflow JSON" value={text} onChange={(event) => setText(event.target.value)} /><div className="modal-actions"><button className="button" onClick={download}><Download size={15} />Export JSON</button><button className="button primary" onClick={submit}>Apply changes</button></div></Modal>;
}
