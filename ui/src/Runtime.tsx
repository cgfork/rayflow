import { useMemo, useState } from 'react';
import { Activity, ArrowUpRight, Box, CircleCheck, Clock3, GitBranch, Play, RotateCcw, Workflow } from 'lucide-react';
import { api, dateTime, relativeTime, send, shortId, usePaged, useRemote } from './api';
import { Back, Badge, Details, Empty, ErrorBanner, Loading, Modal, PageHeader, Pagination, Refresh, SearchBox } from './components';
import FlowCanvas from './FlowCanvas';
import type { Graph, Page, Run, RunView } from './types';

export default function Runtime({ notify, openJobs }: { notify: (message: string) => void; openJobs: (runId: string) => void }) {
  const [selected, setSelected] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);
  const [search, setSearch] = useState('');
  const result = usePaged<Run>('/runs', 5000);
  const rows = result.data?.items.filter((run) => `${run.uuid} ${run.graph_id} ${run.data_id}`.includes(search.toLowerCase())) ?? [];
  if (selected) return <RunDetail id={selected} close={() => setSelected(null)} notify={notify} openJobs={openJobs} />;
  return <>
    <PageHeader eyebrow="OBSERVE & OPERATE" title="Runtime" description="Follow every workflow, from its first node to its final result."><button className="button primary" onClick={() => setStarting(true)}><Play size={16} />Start a run</button></PageHeader>
    <div className="section-callout"><Activity size={19} /><span>State follows execution.</span><p>Inspect node states and resume or rerun work as it progresses.</p><span className="live-label"><i />Refreshes every 5s</span></div>
    <ErrorBanner error={result.error} retry={result.refresh} />
    <section className="panel"><div className="panel-toolbar"><h2>Workflow runs <span className="count">{result.data?.items.length ?? '—'}</span></h2><div className="actions"><SearchBox value={search} onChange={setSearch} placeholder="Find a run, graph, or data ID…" /><Refresh loading={result.loading} onClick={result.refresh} /></div></div>
      {!result.data && result.loading ? <Loading /> : !rows.length ? <Empty title={search ? 'No matching runs' : 'No workflow runs yet'} action={!search ? <button className="button primary" onClick={() => setStarting(true)}><Play size={15} />Start a run</button> : undefined}>{search ? 'Search the IDs on this page, or move to an older page.' : 'Publish a workflow, prepare its node configurations, and start an execution.'}</Empty> : <div className="table-scroll"><table><thead><tr><th>Run</th><th>Graph</th><th>Version</th><th>Revision</th><th>Updated</th><th /></tr></thead><tbody>{rows.map((run) => <tr key={run.uuid}><td><button className="table-name" onClick={() => setSelected(run.uuid)}><span className="table-icon"><Activity size={18} /></span><span><strong>Run {shortId(run.uuid)}</strong><small>Data {shortId(run.data_id)}</small></span></button></td><td className="mono" title={run.graph_id}>{shortId(run.graph_id)}</td><td><Badge status="published">v{run.version}</Badge></td><td>#{run.revision}</td><td title={dateTime(run.updated_at)}>{relativeTime(run.updated_at)}</td><td><button className="icon-button" aria-label={`Inspect run ${shortId(run.uuid)}`} onClick={() => setSelected(run.uuid)}><ArrowUpRight size={18} /></button></td></tr>)}</tbody></table></div>}
      <Pagination page={result.page} count={rows.length} more={result.data?.next_cursor != null} next={result.next} previous={result.previous} />
    </section>
    <div className="section-note"><GitBranch size={14} />Each run keeps its published graph version. Commands are applied asynchronously by the pilot.</div>
    {starting && <StartRun close={() => setStarting(false)} completed={() => { setStarting(false); result.refresh(); notify('Start command accepted. The run will appear after the pilot processes it.'); }} />}
  </>;
}

function StartRun({ close, completed }: { close: () => void; completed: () => void }) {
  const graphs = useRemote<Page<Graph>>('/graphs?limit=200');
  const [graphId, setGraphId] = useState('');
  const [dataId, setDataId] = useState<string>(() => crypto.randomUUID());
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const unique = (graphs.data?.items ?? []).filter((graph, index, items) => items.findIndex((item) => item.id === graph.id) === index);
  async function start(event: React.FormEvent) {
    event.preventDefault(); setBusy(true); setError(undefined);
    try {
      await api<Graph>(`/graphs/${graphId}`);
      await send('/runs/start', { graph_id: graphId, data_id: dataId, nodes: [] });
      completed();
    } catch (failure) { setError((failure as Error).message); }
    finally { setBusy(false); }
  }
  return <Modal title="Start a workflow run" close={() => { if (!busy) close(); }}>
    <form onSubmit={(event) => void start(event)}>
      <p className="muted">Starts at the root of the latest published version using its node settings.</p>
      <ErrorBanner error={error || graphs.error} retry={graphs.refresh} />
      <label className="field">Published workflow
        <select value={unique.some((graph) => graph.id === graphId) ? graphId : ''} onChange={(event) => setGraphId(event.target.value)} disabled={busy}>
          <option value="">Choose a workflow…</option>
          {unique.map((graph) => <option key={graph.id} value={graph.id}>{graph.name} · {shortId(graph.id)}</option>)}
        </select>
      </label>
      <label className="field">Graph ID<input value={graphId} required pattern="[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}" onChange={(event) => setGraphId(event.target.value)} disabled={busy} placeholder="Or paste a published graph UUID" /></label>
      <label className="field">Data ID<input value={dataId} required pattern="[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}" onChange={(event) => setDataId(event.target.value)} disabled={busy} /></label>
      <p className="field-hint">Use a new data ID for an independent execution.</p>
      <div className="modal-actions"><button className="button" type="button" onClick={close} disabled={busy}>Cancel</button><button className="button primary" type="submit" disabled={busy || !graphId}><Play size={15} />{busy ? 'Submitting…' : 'Start run'}</button></div>
    </form>
  </Modal>;
}

function RunDetail({ id, close, notify, openJobs }: { id: string; close: () => void; notify: (message: string) => void; openJobs: (runId: string) => void }) {
  const result = useRemote<RunView>(`/runs/${id}`, 5000);
  const [selected, setSelected] = useState<string | null>(null);
  const [action, setAction] = useState<'rerun' | 'resume' | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const statuses = useMemo(() => Object.fromEntries(result.data?.graph.nodes.map((node) => [node.id, result.data?.state.nodes.find((state) => state.node_id === node.id)?.status ?? 'Waiting']) ?? []), [result.data]);
  const node = result.data?.graph.nodes.find((item) => item.id === selected);
  const state = result.data?.state.nodes.find((item) => item.node_id === selected);
  async function command() {
    if (!result.data || !node || !action) return;
    setBusy(true); setError(undefined);
    try {
      await send(`/runs/${action}`, { graph_id: result.data.state.graph_id, data_id: result.data.state.data_id, node: node.id });
      notify(`${action === 'resume' ? 'Resume' : 'Rerun'} command accepted for ${node.name}. Waiting for the pilot.`);
      setAction(null); result.refresh();
    } catch (failure) { setError((failure as Error).message); }
    finally { setBusy(false); }
  }
  return <>
    <Back onClick={close}>All runs</Back>
    <PageHeader eyebrow={`RUN ${shortId(id)}`} title={result.data?.graph.name ?? 'Workflow execution'} description="Live node state for this run’s pinned graph version."><button className="button" onClick={() => openJobs(id)}><Box size={15} />View jobs</button><Refresh loading={result.loading} onClick={result.refresh} /></PageHeader>
    <ErrorBanner error={result.error || error} retry={result.refresh} />
    {!result.data ? result.loading ? <Loading /> : null : <>
      <div className="metric-grid"><div className="metric"><span><Workflow size={17} />Graph version</span><strong>v{result.data.state.version}</strong><small>Revision {result.data.state.revision}</small></div><div className="metric"><span><Activity size={17} />In progress</span><strong>{Object.values(statuses).filter((status) => status === 'Running' || status === 'Ready').length}</strong><small>Running or ready</small></div><div className="metric"><span><CircleCheck size={17} />Succeeded</span><strong>{Object.values(statuses).filter((status) => status === 'Succeeded').length}<em> / {result.data.graph.nodes.length}</em></strong><small>Completed nodes</small></div><div className="metric"><span><Clock3 size={17} />Needs attention</span><strong>{Object.values(statuses).filter((status) => ['Pending', 'Failed', 'Terminated'].includes(status)).length}</strong><small>Pending, failed, or terminated</small></div></div>
      {!result.data.current && <div className="info-banner">This is a historical run version. Open the current version to issue node commands.</div>}
      <div className="studio-body runtime-canvas"><div className="canvas-panel"><div className="canvas-toolbar"><span><GitBranch size={15} />Execution graph</span><span className="live-label"><i />Refreshes every 5s</span></div><FlowCanvas graph={result.data.graph} statuses={statuses} selected={selected} select={setSelected} /><div className="canvas-caption"><span>Data <code>{shortId(result.data.state.data_id)}</code></span><span>Select a node to inspect its state</span></div></div><aside className="inspector"><div className="inspector-heading">NODE STATE<Activity size={16} /></div>{node ? <div className="inspector-fields"><h3>{node.name}</h3><Badge status={state?.status ?? 'waiting'}>{state?.status ?? 'Waiting'}</Badge><Details items={[
        ['Type', node.node_type], ['Node ID', <code>{node.id}</code>], ['Revision', state ? `#${state.revision}` : 'Not started'], ['Updated', dateTime(state?.updated_at)], ['Inputs', node.inputs.length], ['Outputs', node.outputs.length],
      ]} /><div className="vertical-actions"><button className="button primary" disabled={!result.data.current || state?.status !== 'Pending' || busy || !!result.error} onClick={() => setAction('resume')}><Play size={15} />Resume node</button><button className="button" disabled={!result.data.current || !state || !['Succeeded', 'Failed', 'Terminated', 'Pending'].includes(state.status) || busy || !!result.error} onClick={() => setAction('rerun')}><RotateCcw size={15} />Rerun node</button></div><p className="field-hint">Resume continues a pending node. Rerun creates a new revision.</p></div> : <div className="inspector-empty"><Activity size={28} /><h3>Every node tells a story</h3><p>Select one to see its current state and available actions.</p></div>}</aside></div>
    </>}
    {action && node && <Modal title={`${action === 'resume' ? 'Resume' : 'Rerun'} ${node.name}?`} close={() => { if (!busy) setAction(null); }}><p>{action === 'resume' ? 'The pending node will become ready for scheduling again.' : 'The node will be reset to Ready at a new revision.'}</p><ErrorBanner error={error} /><div className="modal-actions"><button className="button" disabled={busy} onClick={() => setAction(null)}>Cancel</button><button className="button primary" disabled={busy} onClick={() => void command()}>{busy ? 'Submitting…' : 'Confirm command'}</button></div></Modal>}
  </>;
}
