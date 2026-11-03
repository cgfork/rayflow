import { useState } from 'react';
import { ArrowUpRight, Box, Clock3, Cpu, Filter, HardDrive, ListTodo, ShieldX, X } from 'lucide-react';
import { dateTime, memory, relativeTime, send, shortId, usePaged } from './api';
import { Badge, Details, Empty, ErrorBanner, Modal, PageHeader, Pagination, Refresh, SearchBox } from './components';
import type { Job, JobStatus } from './types';
import { Loading } from './components';

const statuses: JobStatus[] = ['Scheduled', 'Polled', 'Yielded', 'Succeeded', 'Failed', 'Terminated'];
export default function Schedule({ notify, filter, clearFilter }: { notify: (message: string) => void; filter: { agent_id?: string; run_id?: string }; clearFilter: () => void }) {
  const [status, setStatus] = useState('');
  const [search, setSearch] = useState('');
  const [selected, setSelected] = useState<Job | null>(null);
  const [action, setAction] = useState<'cancel' | 'terminate' | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const params = new URLSearchParams(filter);
  if (status) params.set('status', status);
  const result = usePaged<Job>(`/jobs?${params}`, 5000);
  const rows = result.data?.items.filter((job) => `${job.uuid} ${job.run_id} ${job.agent_id ?? ''}`.includes(search.toLowerCase())) ?? [];
  const job = selected && (result.data?.items.find((item) => item.uuid === selected.uuid) ?? selected);
  async function perform() {
    if (!job || !action || !job.agent_id) return;
    setBusy(true); setError(undefined);
    try {
      await send(`/job/${action}`, { job_id: job.uuid, agent_id: job.agent_id });
      notify(`Job ${shortId(job.uuid)} terminated. Its task event is queued for the pilot.`);
      setAction(null); setSelected(null); result.refresh();
    } catch (failure) { setError((failure as Error).message); result.refresh(); }
    finally { setBusy(false); }
  }
  return <>
    <PageHeader eyebrow="DISPATCH & CONTROL" title="Schedule" description="Track queued work, agent claims, and completed jobs."><Refresh loading={result.loading} onClick={result.refresh} /></PageHeader>
    <div className="metric-grid three"><div className="metric"><span><ListTodo size={17} />Scheduled</span><strong>{result.data ? result.data.items.filter((item) => item.status === 'Scheduled').length : '—'}</strong><small>On this page · waiting for an agent</small></div><div className="metric"><span><Cpu size={17} />Claimed</span><strong>{result.data ? result.data.items.filter((item) => item.status === 'Polled').length : '—'}</strong><small>On this page · owned by an agent</small></div><div className="metric"><span><Clock3 size={17} />Finished</span><strong>{result.data ? result.data.items.filter((item) => !['Scheduled', 'Polled'].includes(item.status)).length : '—'}</strong><small>On this page · terminal jobs</small></div></div>
    <ErrorBanner error={result.error} retry={result.refresh} />
    {(filter.agent_id || filter.run_id) && <div className="filter-chip"><Filter size={14} />{filter.agent_id ? `Agent ${shortId(filter.agent_id)}` : `Run ${shortId(filter.run_id!)}`}<button className="icon-button" aria-label="Clear job filter" onClick={clearFilter}><X size={13} /></button></div>}
    <section className="panel"><div className="panel-toolbar"><h2>Job queue</h2><div className="actions"><select aria-label="Filter jobs by status" value={status} onChange={(event) => setStatus(event.target.value)}><option value="">All states</option>{statuses.map((item) => <option key={item}>{item}</option>)}</select><SearchBox value={search} onChange={setSearch} placeholder="Find a job, run, or agent ID…" /></div></div>
      {!result.data && result.loading ? <Loading /> : !rows.length ? <Empty title="No jobs in this view">Jobs appear when the pilot schedules ready nodes. Try another state or clear your filters.</Empty> : <div className="table-scroll"><table><thead><tr><th>Job</th><th>State</th><th>Agent</th><th>Resources</th><th>Updated</th><th /></tr></thead><tbody>{rows.map((item) => <tr key={item.uuid}><td><button className="table-name" onClick={() => { setSelected(item); setError(undefined); }}><span className="table-icon"><Box size={18} /></span><span><strong>{shortId(item.uuid)}</strong><small>Run {shortId(item.run_id)}</small></span></button></td><td><Badge status={item.status}>{item.status}</Badge></td><td className="mono" title={item.agent_id ?? ''}>{item.agent_id ? shortId(item.agent_id) : 'Unassigned'}</td><td><div className="resource-cell"><span><Cpu size={13} />{item.required_cpu_cores} cores</span><span><HardDrive size={13} />{memory(item.required_memory_bytes)}</span></div></td><td title={dateTime(item.updated_at)}>{relativeTime(item.updated_at)}</td><td><button className="icon-button" aria-label={`Inspect job ${shortId(item.uuid)}`} onClick={() => { setSelected(item); setError(undefined); }}><ArrowUpRight size={18} /></button></td></tr>)}</tbody></table></div>}
      <Pagination page={result.page} count={rows.length} more={result.data?.next_cursor != null} previous={result.previous} next={result.next} />
    </section><div className="section-note"><Clock3 size={14} />Claims use pinned job leases. Existing agent ownership is required for cancellation and termination.</div>
    {job && <Modal title={`Job ${shortId(job.uuid)}`} close={() => { if (!busy) { setSelected(null); setAction(null); } }}><Badge status={job.status}>{job.status}</Badge><Details items={[
      ['Job ID', <code>{job.uuid}</code>], ['Run ID', <code>{job.run_id}</code>], ['Task ID', <code>{job.task_id}</code>], ['Agent', <code>{job.agent_id ?? 'Unassigned'}</code>], ['Resources', `${job.required_cpu_cores} cores · ${memory(job.required_memory_bytes)}`], ['Created', dateTime(job.created_at)], ['Last delivery', dateTime(job.polled_at)], ['Expires', dateTime(job.expired_at)],
    ]} /><ErrorBanner error={error} />{job.status === 'Polled' && job.agent_id ? <>{action ? <div className="confirm-action"><strong>{action === 'cancel' ? 'Cancel' : 'Terminate'} this job?</strong><p>Records a terminal event. The agent stops the container when it reconciles the claim or reaches its expiry.</p><div className="actions"><button className="button" disabled={busy} onClick={() => setAction(null)}>Go back</button><button className="button danger" disabled={busy || !!result.error} onClick={() => void perform()}>{busy ? 'Submitting…' : 'Confirm action'}</button></div></div> : <div className="modal-actions"><button className="button" disabled={!!result.error} onClick={() => setAction('cancel')}><X size={15} />Cancel job</button><button className="button danger" disabled={!!result.error} onClick={() => setAction('terminate')}><ShieldX size={15} />Terminate</button></div>}</> : <p className="info-banner">{job.status === 'Scheduled' ? 'Waiting for an agent claim. Claim actions become available after delivery.' : 'This job has reached a terminal state. Use Runtime to rerun its node.'}</p>}</Modal>}
  </>;
}
