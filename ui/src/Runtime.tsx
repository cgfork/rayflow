import { randomUUID } from './uuid';
import { useEffect, useMemo, useRef, useState, type RefObject } from 'react';
import { Activity, ArrowRight, ArrowUpCircle, ArrowUpRight, Box, CircleCheck, Clock3, Download, GitBranch, Play, RotateCcw, Workflow } from 'lucide-react';
import { api, dateTime, isHttpDownloadUrl, relativeTime, send, shortId, usePaged, useRemote } from './api';
import { Back, Badge, Details, Empty, ErrorBanner, Loading, Modal, PageHeader, Pagination, Refresh, SearchBox, UuidCopy, UuidText } from './components';
import FlowCanvas from './FlowCanvas';
import RuntimeNodeArtifacts, { useNodeArchives } from './RuntimeNodeArtifacts';
import WorkflowPicker from './WorkflowPicker';
import type { Graph, GraphNode, NodeRun, Run, RunFileTransfer, RunView } from './types';

type RunAction = 'rerun' | 'resume';

function findNodeRun(view: RunView | undefined, nodeId: string | null) {
  return view?.node_runs.find(([nodeRun]) => nodeRun.node_id === nodeId);
}

function canRunCommand(run: Run, nodeRun: NodeRun | undefined, action: RunAction) {
  if (!nodeRun || nodeRun.revision > run.revision) return false;
  return action === 'resume'
    ? nodeRun.revision === run.revision && nodeRun.status === 'Pending'
    : ['Ready', 'Succeeded', 'Failed', 'Terminated'].includes(nodeRun.status);
}

export default function Runtime({ notify, openJobs, graphId, onGraphChange }: {
  notify: (message: string) => void;
  openJobs: (runId: string) => void;
  graphId?: string;
  onGraphChange: (graphId: string | undefined) => void;
}) {
  const [selected, setSelected] = useState<string | null>(null);
  const [rerunning, setRerunning] = useState<string | null>(null);
  const [upgrading, setUpgrading] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);
  const [search, setSearch] = useState('');
  const graph = useRemote<Graph>(graphId ? `/graphs/${encodeURIComponent(graphId)}` : null);
  const workflow = graphId ? { id: graphId, name: graph.data?.name ?? `Workflow ${graphId}` } : null;
  const workflowName = graph.data?.name ?? (graphId ? `Workflow ${shortId(graphId)}` : '');
  const path = graphId ? `/runs?graph_id=${encodeURIComponent(graphId)}` : null;
  const result = usePaged<Run>(path);
  const rows = result.data?.items.filter((run) => `${run.uuid} ${run.data_id}`.toLowerCase().includes(search.toLowerCase().trim())) ?? [];
  useEffect(() => {
    setSelected(null); setSearch(''); setStarting(false); setRerunning(null); setUpgrading(null);
  }, [graphId]);
  if (selected) return <RunDetail id={selected} close={() => setSelected(null)} notify={notify} openJobs={openJobs} />;
  return <>
    <div className="runtime-heading"><PageHeader eyebrow="OBSERVE & OPERATE" title="Runtime" description="Follow a workflow, from its first node to its final result."><WorkflowPicker value={workflow && { ...workflow, name: workflowName }} onChange={(choice) => onGraphChange(choice?.id)} /><button className="button primary" disabled={!workflow || !!graph.error} onClick={() => setStarting(true)}><Play size={16} />Start a run</button></PageHeader></div>
    {!workflow ? <section className="panel"><Empty title="Choose a workflow">Search for a published workflow in the header to view its runs.</Empty></section> : <>
    <div className="section-callout runtime-workflow-callout"><Activity size={19} /><span title={workflow.id}>{workflowName}</span><p>Inspect node states and resume or rerun work as it progresses.</p></div>
    <ErrorBanner error={graph.error} retry={graph.refresh} />
    <ErrorBanner error={result.error} retry={result.refresh} />
    <section className="panel runtime-runs"><div className="panel-toolbar"><h2>Workflow runs <span className="count">{result.data?.items.length ?? '—'}</span></h2><div className="actions"><SearchBox value={search} onChange={setSearch} placeholder="Find a run or data ID…" /><Refresh loading={result.loading} onClick={result.refresh} /></div></div>
      {!result.data && result.loading ? <Loading /> : !rows.length ? <Empty title={search ? 'No matching runs' : 'No runs for this workflow'} action={!search && !result.error && !graph.error ? <button className="button primary" onClick={() => setStarting(true)}><Play size={15} />Start a run</button> : undefined}>{search ? 'Search the IDs on this page, or move to an older page.' : 'Start an execution using this workflow’s latest published version.'}</Empty> : <div className="table-scroll"><table><thead><tr><th>Run</th><th>Version</th><th>Revision</th><th>Updated</th><th /></tr></thead><tbody>{rows.map((run) => <tr key={run.uuid}><td><RunIdentifier run={run} open={() => setSelected(run.uuid)} notify={notify} /></td><td><Badge status="published">v{run.version}</Badge></td><td>#{run.revision}</td><td title={dateTime(run.updated_at)}>{relativeTime(run.updated_at)}</td><td><div className="actions"><button className="button" aria-label={`Upgrade run ${run.uuid}`} disabled={!!result.error} onClick={() => setUpgrading(run.uuid)}><ArrowUpCircle size={15} />Upgrade</button><button className="button" aria-label={`Rerun run ${run.uuid}`} disabled={!!result.error} onClick={() => setRerunning(run.uuid)}><RotateCcw size={15} />Rerun</button><button className="icon-button" aria-label={`Inspect run ${run.uuid}`} onClick={() => setSelected(run.uuid)}><ArrowUpRight size={18} /></button></div></td></tr>)}</tbody></table></div>}
      <Pagination page={result.page} count={rows.length} more={result.data?.next_cursor != null} next={result.next} previous={result.previous} />
    </section>
    <div className="section-note"><GitBranch size={14} />Upgrade moves a run to a newer published version from a shared node. Commands are applied asynchronously by the pilot.</div>
    {starting && <StartRun workflow={workflow} close={() => setStarting(false)} completed={() => { setStarting(false); result.refresh(); notify('Start command accepted. The run will appear after the pilot processes it.'); }} />}
    {rerunning && <RunCommand id={rerunning} action="rerun" close={() => setRerunning(null)} completed={(message) => { setRerunning(null); result.refresh(); notify(message); }} />}
    {upgrading && <UpgradeRun id={upgrading} close={() => setUpgrading(null)} completed={(message) => { setUpgrading(null); result.refresh(); notify(message); }} />}
    </>}
  </>;
}

function RunIdentifier({ run, open, notify }: { run: Run; open: () => void; notify: (message: string) => void }) {
  return <div className="run-identifier">
    <span className="table-icon" aria-hidden="true"><Activity size={18} /></span>
    <span className="identifier-wrap">
      <UuidCopy id={run.uuid} label="Run" notify={notify}>
        <button className="table-name uuid-copy-label" type="button" onClick={open}><strong title={run.uuid}>Run {shortId(run.uuid)}</strong></button>
      </UuidCopy>
      <UuidCopy id={run.data_id} label="Data" notify={notify}>
        <button className="table-name uuid-copy-label" type="button" onClick={open}><small title={run.data_id}>Data {shortId(run.data_id)}</small></button>
      </UuidCopy>
    </span>
  </div>;
}

function StartRun({ close, completed, workflow }: { close: () => void; completed: () => void; workflow: Pick<Graph, 'id' | 'name'> }) {
  const graphId = workflow.id;
  const [dataId, setDataId] = useState<string>(() => randomUUID());
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
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
      <ErrorBanner error={error} />
      <label className="field">Workflow<input value={workflow.name} readOnly /></label>
      <label className="field">Data ID<input value={dataId} required pattern="[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}" onChange={(event) => setDataId(event.target.value)} disabled={busy} /></label>
      <p className="field-hint">Use a new data ID for an independent execution.</p>
      <div className="modal-actions"><button className="button" type="button" onClick={close} disabled={busy}>Cancel</button><button className="button primary" type="submit" disabled={busy || !graphId}><Play size={15} />{busy ? 'Submitting…' : 'Start run'}</button></div>
    </form>
  </Modal>;
}

function RunDetail({ id, close, notify, openJobs }: { id: string; close: () => void; notify: (message: string) => void; openJobs: (runId: string) => void }) {
  const result = useRemote<RunView>(`/runs/${id}`);
  const inspector = useRef<HTMLElement>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [upgrading, setUpgrading] = useState(false);
  const [action, setAction] = useState<{ type: RunAction; nodeId?: string } | null>(null);
  const view = result.data;
  const statuses = useMemo(() => {
    const byNodeId = new Map(view?.node_runs.map(([nodeRun]) => [nodeRun.node_id, nodeRun.status]));
    return Object.fromEntries(view?.graph.nodes.map((node) => [node.id, byNodeId.get(node.id) ?? 'Waiting']) ?? []);
  }, [view]);
  const [nodeRun, nodeInfo] = findNodeRun(view, selected) ?? [];
  const archives = useNodeArchives(id, nodeRun?.uuid);
  const node = nodeInfo ?? view?.graph.nodes.find((item) => item.id === selected);
  const canRerun = view?.node_runs.some(([entry]) => canRunCommand(view.run, entry, 'rerun'));
  function refresh() {
    result.refresh();
    if (nodeRun) {
      archives.result.refresh();
      archives.usage.refresh();
    }
  }
  return <>
    <Back onClick={close}>All runs</Back>
    <div className="runtime-run-heading"><PageHeader eyebrow={<UuidCopy id={id} label="Run" notify={notify}><span title={id}>RUN {shortId(id)}</span></UuidCopy>} title={view?.graph.name ?? 'Workflow execution'} description="Node state for this run’s pinned graph version."><button className="button primary" disabled={!canRerun || !!result.error} onClick={() => setAction({ type: 'rerun' })}><RotateCcw size={15} />Rerun</button><button className="button" disabled={!view || !!result.error} onClick={() => setUpgrading(true)}><ArrowUpCircle size={15} />Upgrade</button><button className="button" onClick={() => openJobs(id)}><Box size={15} />View jobs</button><Refresh loading={result.loading} onClick={refresh} /></PageHeader></div>
    <ErrorBanner error={result.error} retry={result.refresh} />
    {!view ? result.loading ? <Loading /> : null : <>
      <div className="metric-grid"><div className="metric"><span><Workflow size={17} />Graph version</span><strong>v{view.run.version}</strong><small>Revision {view.run.revision}</small></div><div className="metric"><span><Activity size={17} />In progress</span><strong>{Object.values(statuses).filter((status) => status === 'Running' || status === 'Ready').length}</strong><small>Running or ready</small></div><div className="metric"><span><CircleCheck size={17} />Succeeded</span><strong>{Object.values(statuses).filter((status) => status === 'Succeeded').length}<em> / {view.graph.nodes.length}</em></strong><small>Completed nodes</small></div><div className="metric"><span><Clock3 size={17} />Needs attention</span><strong>{Object.values(statuses).filter((status) => ['Pending', 'Failed', 'Terminated'].includes(status)).length}</strong><small>Pending, failed, or terminated</small></div></div>
      <div className="studio-body runtime-canvas"><div className="canvas-panel"><div className="canvas-toolbar"><span><GitBranch size={15} />Execution graph</span></div><FlowCanvas graph={view.graph} statuses={statuses} selected={selected} select={setSelected} /><div className="canvas-caption"><span>Data <UuidCopy id={view.run.data_id} label="Data" notify={notify}><UuidText id={view.run.data_id} /></UuidCopy></span><span>Select a node to inspect its state</span></div></div><aside className="inspector" ref={inspector}><div className="inspector-heading">NODE STATE<Activity size={16} /></div>{node ? <div className="inspector-fields"><h3>{node.name}</h3><Badge status={nodeRun?.status ?? 'waiting'}>{nodeRun?.status ?? 'Waiting'}</Badge>
      <div className="vertical-actions"><button className="button primary" disabled={!canRunCommand(view.run, nodeRun, 'resume') || !!result.error} onClick={() => setAction({ type: 'resume', nodeId: node.id })}><Play size={15} />Resume node</button><button className="button" disabled={!canRunCommand(view.run, nodeRun, 'rerun') || !!result.error} onClick={() => setAction({ type: 'rerun', nodeId: node.id })}><RotateCcw size={15} />Rerun node</button></div><p className="field-hint">Resume continues a pending node. Rerun creates a new revision.</p>
      <RuntimeNodeDetails key={`${id}-${node.id}-${view.run.revision}`} runId={id} node={node} nodeRun={nodeRun} revision={view.run.revision} inspector={inspector} />
      {nodeRun && <RuntimeNodeArtifacts {...archives} />}</div> : <div className="inspector-empty"><Activity size={28} /><h3>Every node tells a story</h3><p>Select one to see its current state and available actions.</p></div>}</aside></div>
    </>}
    {action && <RunCommand id={id} action={action.type} initialNode={action.nodeId} close={() => setAction(null)} completed={(message) => { setAction(null); result.refresh(); notify(message); }} />}
    {upgrading && <UpgradeRun id={id} close={() => setUpgrading(false)} completed={(message) => { setUpgrading(false); result.refresh(); notify(message); }} />}
  </>;
}

type FileDirection = 'input' | 'output';

function RuntimeNodeDetails({ runId, node, nodeRun, revision, inspector }: {
  runId: string; node: GraphNode; nodeRun?: NodeRun; revision: number; inspector: RefObject<HTMLElement | null>;
}) {
  const [drawer, setDrawer] = useState<FileDirection | null>(null);
  const [busy, setBusy] = useState<string>();
  const [error, setError] = useState<{ direction: FileDirection; message: string }>();
  const files = drawer === 'input' ? node.inputs : node.outputs;
  const canDownload = drawer === 'input' || nodeRun?.status === 'Succeeded';
  async function download(direction: FileDirection, fileName: string) {
    setBusy(`${direction}:${fileName}`); setError(undefined);
    try {
      const transfer = await send<RunFileTransfer>(`/runs/${runId}/files/download`, { node_id: node.id, direction, file_name: fileName, revision });
      const signed = transfer?.presigned;
      if (signed?.operation !== 'download' || !isHttpDownloadUrl(signed.url)) {
        throw new Error('The server did not return a valid download link.');
      }
      const anchor = document.createElement('a');
      anchor.href = signed.url;
      anchor.download = fileName;
      anchor.target = '_blank';
      anchor.rel = 'noopener noreferrer';
      document.body.append(anchor);
      try { anchor.click(); }
      finally { anchor.remove(); }
    } catch (failure) { setError({ direction, message: (failure as Error).message }); }
    finally { setBusy(undefined); }
  }
  function fileControl(direction: FileDirection) {
    const count = direction === 'input' ? node.inputs.length : node.outputs.length;
    return <span className="runtime-file-control"><span>{count}</span><button
      className="icon-button" type="button" aria-label={`View ${direction} files`} aria-haspopup="dialog"
      onClick={(event) => { event.currentTarget.focus({ preventScroll: true }); setDrawer(direction); }}
    ><ArrowRight size={16} aria-hidden="true" /></button></span>;
  }
  return <>
    <Details items={[
      ['Type', node.node_type], ['Node ID', <UuidText id={node.id} />], ['Revision', nodeRun ? `#${nodeRun.revision}` : 'Not started'], ['Updated', dateTime(nodeRun?.updated_at)],
      ['Inputs', fileControl('input')], ['Outputs', fileControl('output')],
    ]} />
    <ErrorBanner error={error?.direction === drawer ? undefined : error?.message} />
    {drawer && <Modal title={drawer === 'input' ? 'Input files' : 'Output files'} presentation="drawer" drawerAnchor={inspector} close={() => setDrawer(null)}>
      <ErrorBanner error={error?.direction === drawer ? error.message : undefined} />
      {files.length ? <>
        {!canDownload && <p className="muted small-text">Output files are available after the node succeeds.</p>}
        <ul className="runtime-files">{files.map((file) => <li className="runtime-file" key={file.file_name}>
          <span title={file.file_name}>{file.file_name}</span>
          {canDownload && <button type="button" className="button" aria-label={`Download ${drawer} ${file.file_name}`} disabled={!!busy} onClick={() => void download(drawer, file.file_name)}><Download size={14} />{busy === `${drawer}:${file.file_name}` ? 'Preparing…' : 'Download'}</button>}
        </li>)}</ul>
      </> : <p className="muted small-text">No {drawer} files.</p>}
    </Modal>}
  </>;
}

function UpgradeRun({ id, close, completed }: { id: string; close: () => void; completed: (message: string) => void }) {
  const result = useRemote<RunView>(`/runs/${id}`);
  const [busy, setBusy] = useState(false);
  return <Modal title="Upgrade workflow" close={() => { if (!busy) close(); }}>
    <ErrorBanner error={result.error} retry={result.refresh} />
    {!result.data ? result.loading ? <Loading /> : null : <UpgradeForm key={result.data.run.version} view={result.data} stale={!!result.error} busy={busy} setBusy={setBusy} close={close} completed={completed} />}
  </Modal>;
}

function UpgradeForm({ view, stale, busy, setBusy, close, completed }: { view: RunView; stale: boolean; busy: boolean; setBusy: (busy: boolean) => void; close: () => void; completed: (message: string) => void }) {
  const { run } = view;
  const versions = usePaged<Graph>(`/graphs?graph_id=${run.graph_id}`);
  const [version, setVersion] = useState('');
  const [nodeId, setNodeId] = useState('');
  const [error, setError] = useState<string>();
  const target = versions.data?.items.find((graph) => String(graph.version) === version && graph.version > run.version);
  const shared = useRemote<string[]>(target ? `/graphs/${run.graph_id}/shared-nodes?from_version=${run.version}&version=${target.version}` : null);
  const nodes = view.graph.nodes.filter((node) => shared.data?.includes(node.id));
  const node = nodes.find((item) => item.id === nodeId);
  const allowed = target && node && !stale && !versions.error && !shared.error && canRunCommand(run, findNodeRun(view, node.id)?.[0], 'rerun');
  const newer = versions.data?.items.filter((graph) => graph.version > run.version).sort((left, right) => right.version - left.version) ?? [];
  async function upgrade(event: React.FormEvent) {
    event.preventDefault();
    if (!allowed || !target || !node || busy) return;
    setBusy(true); setError(undefined);
    try {
      await send('/runs/upgrade', { graph_id: run.graph_id, data_id: run.data_id, version: target.version, node: node.id });
      completed(`Upgrade command accepted for v${target.version} from ${node.name}. Waiting for the pilot.`);
    } catch (failure) { setError((failure as Error).message); }
    finally { setBusy(false); }
  }
  function changePage(next: () => void) {
    if (busy) return;
    setVersion(''); setNodeId(''); setError(undefined); next();
  }
  return <form onSubmit={(event) => void upgrade(event)}>
    <p>Move this run from v{run.version} to a newer published version. Choose a shared node to rerun at a new revision, keeping the run and data IDs.</p>
    <ErrorBanner error={error || versions.error || shared.error} retry={() => { setError(undefined); versions.refresh(); shared.refresh(); }} />
    <label className="field">Target version<select value={version} required disabled={busy || stale || !!versions.error || !versions.data} onChange={(event) => { setVersion(event.target.value); setNodeId(''); setError(undefined); }}>
      <option value="">Choose a newer version…</option>{newer.map((graph) => <option key={graph.version} value={graph.version}>v{graph.version} · {graph.name}</option>)}
    </select></label>
    {!versions.data && versions.loading && <Loading />}
    {versions.data && !versions.error && newer.length === 0 && <p className="info-banner">{versions.page === 1 && versions.data.next_cursor == null ? 'No newer published versions are available.' : 'No newer published versions on this page.'}</p>}
    {(versions.page > 1 || versions.data?.next_cursor != null) && <Pagination page={versions.page} count={newer.length} more={versions.data?.next_cursor != null} next={() => changePage(versions.next)} previous={() => changePage(versions.previous)} />}
    <label className="field">Shared restart node<select value={nodeId} required disabled={busy || stale || !target || !shared.data || !!shared.error} onChange={(event) => { setNodeId(event.target.value); setError(undefined); }}>
      <option value="">Choose a shared node…</option>{nodes.map((item) => {
        const [nodeRun] = findNodeRun(view, item.id) ?? [];
        return <option key={item.id} value={item.id} disabled={!canRunCommand(run, nodeRun, 'rerun')}>{item.name} · {nodeRun?.status ?? 'Waiting'}{nodeRun && nodeRun.revision !== run.revision ? ' (earlier revision)' : ''}</option>;
      })}
    </select></label>
    {target && !shared.data && shared.loading && <Loading />}
    {target && shared.data && !shared.error && <p className="field-hint">{nodes.length === 0 ? 'These versions have no shared restart nodes.' : nodes.some((item) => canRunCommand(run, findNodeRun(view, item.id)?.[0], 'rerun')) ? 'Ready, succeeded, failed, or terminated shared nodes can be rerun. Downstream results become ineffective until those nodes run again.' : 'No shared nodes are eligible to rerun. Running, pending, and waiting nodes cannot be selected.'}</p>}
    <div className="modal-actions"><button className="button" type="button" disabled={busy} onClick={close}>Cancel</button><button className="button primary" type="submit" disabled={busy || !allowed}><ArrowUpCircle size={15} />{busy ? 'Submitting…' : 'Confirm upgrade'}</button></div>
  </form>;
}

function RunCommand({ id, action, initialNode, close, completed }: { id: string; action: RunAction; initialNode?: string; close: () => void; completed: (message: string) => void }) {
  const result = useRemote<RunView>(`/runs/${id}`);
  const [nodeId, setNodeId] = useState(initialNode ?? '');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const view = result.data;
  const node = view?.graph.nodes.find((item) => item.id === nodeId);
  const allowed = view && node && !result.error && canRunCommand(view.run, findNodeRun(view, node.id)?.[0], action);
  const available = view?.node_runs.some(([nodeRun]) => canRunCommand(view.run, nodeRun, action));
  async function command(event: React.FormEvent) {
    event.preventDefault();
    if (!view || !node || !allowed || busy) return;
    setBusy(true); setError(undefined);
    try {
      await send(`/runs/${action}`, { graph_id: view.run.graph_id, data_id: view.run.data_id, node: node.id });
      completed(`${action === 'resume' ? 'Resume' : 'Rerun'} command accepted for ${node.name}. Waiting for the pilot.`);
    } catch (failure) { setError((failure as Error).message); }
    finally { setBusy(false); }
  }
  return <Modal title={action === 'rerun' ? 'Rerun workflow' : `Resume ${node?.name ?? 'node'}?`} close={() => { if (!busy) close(); }}>
    <ErrorBanner error={result.error || error} retry={result.refresh} />
    {!view && result.loading ? <Loading /> : <form onSubmit={(event) => void command(event)}>
      <p>{action === 'rerun' ? 'Choose a node to rerun. It will become Ready at a new revision, keeping this run’s data ID and published graph version.' : 'The pending node will become ready for scheduling again.'}</p>
      {action === 'rerun' && <label className="field">Start from node<select value={nodeId} required disabled={busy || !view || !!result.error} onChange={(event) => { setNodeId(event.target.value); setError(undefined); }}><option value="">Choose a node…</option>{view?.graph.nodes.map((item) => {
        const [nodeRun] = findNodeRun(view, item.id) ?? [];
        return <option key={item.id} value={item.id} disabled={!canRunCommand(view.run, nodeRun, action)}>{item.name} · {nodeRun?.status ?? 'Waiting'}{nodeRun && nodeRun.revision !== view.run.revision ? ' (earlier revision)' : ''}</option>;
      })}</select></label>}
      {view && !available && <p className="info-banner">No nodes are eligible for this command. Pending nodes in the current revision can be resumed.</p>}
      <div className="modal-actions"><button className="button" type="button" disabled={busy} onClick={close}>Cancel</button><button className="button primary" type="submit" disabled={busy || !allowed}>{busy ? 'Submitting…' : 'Confirm command'}</button></div>
    </form>}
  </Modal>;
}
