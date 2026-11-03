import { useState } from 'react';
import { ArrowUpRight, Box, Cpu, HardDrive, Radio, Server, Wifi, WifiOff } from 'lucide-react';
import { dateTime, memory, relativeTime, shortId, usePaged } from './api';
import { Badge, Details, Empty, ErrorBanner, Loading, Modal, PageHeader, Pagination, Refresh, SearchBox } from './components';
import type { Agent } from './types';

export default function Agents({ openJobs }: { openJobs: (id: string) => void }) {
  const [filter, setFilter] = useState('');
  const [search, setSearch] = useState('');
  const [selected, setSelected] = useState<Agent | null>(null);
  const result = usePaged<Agent>(`/agents${filter ? `?online=${filter}` : ''}`, 5000);
  const rows = result.data?.items.filter((item) => item.uuid.includes(search.toLowerCase())) ?? [];
  const agent = selected && (result.data?.items.find((item) => item.uuid === selected.uuid) ?? selected);
  return <>
    <PageHeader eyebrow="FLEET VISIBILITY" title="Agents" description="A shared view of the agents connected to your runtime."><Refresh loading={result.loading} onClick={result.refresh} /></PageHeader>
    <div className="metric-grid three"><div className="metric"><span><Wifi size={17} />Online</span><strong>{result.data ? result.data.items.filter((item) => item.online).length : '—'}</strong><small>On this page · recently polled</small></div><div className="metric"><span><WifiOff size={17} />Offline</span><strong>{result.data ? result.data.items.filter((item) => !item.online).length : '—'}</strong><small>On this page · no recent poll</small></div><div className="metric"><span><Cpu size={17} />Online capacity</span><strong>{result.data ? result.data.items.filter((item) => item.online).reduce((sum, item) => sum + item.cpu_cores, 0) : '—'}<em> cores</em></strong><small>On this page · total reported budget</small></div></div>
    <ErrorBanner error={result.error} retry={result.refresh} />
    <section className="panel"><div className="panel-toolbar"><h2>Agent fleet</h2><div className="actions"><select aria-label="Filter agents by status" value={filter} onChange={(event) => setFilter(event.target.value)}><option value="">All agents</option><option value="true">Online</option><option value="false">Offline</option></select><SearchBox value={search} onChange={setSearch} placeholder="Search agent ID…" /></div></div>
      {!result.data && result.loading ? <Loading /> : !rows.length ? <Empty title="No agents in this view">Agents appear automatically after their first valid poll. Start an agent with a stable UUID and point it at this server.</Empty> : <div className="table-scroll"><table><thead><tr><th>Agent</th><th>Connection</th><th>Capacity</th><th>Active jobs</th><th>Last seen</th><th /></tr></thead><tbody>{rows.map((item) => <tr key={item.uuid}><td><button className="table-name" onClick={() => setSelected(item)}><span className={`table-icon ${item.online ? 'green' : ''}`}><Server size={18} /></span><span><strong>Agent {shortId(item.uuid)}</strong><small>Since {new Date(item.first_seen_at).toLocaleDateString()}</small></span></button></td><td><Badge status={item.online ? 'online' : 'offline'}>{item.online ? 'Online' : 'Offline'}</Badge></td><td><div className="resource-cell"><span><Cpu size={13} />{item.cpu_cores} cores</span><span><HardDrive size={13} />{memory(item.memory_bytes)}</span></div></td><td><span className="inline-meta"><Box size={14} />{item.active_jobs}<small>reported</small></span></td><td title={dateTime(item.last_seen_at)}>{relativeTime(item.last_seen_at)}</td><td><button className="icon-button" aria-label={`Inspect agent ${shortId(item.uuid)}`} onClick={() => setSelected(item)}><ArrowUpRight size={18} /></button></td></tr>)}</tbody></table></div>}
      <Pagination page={result.page} count={rows.length} more={result.data?.next_cursor != null} previous={result.previous} next={result.next} />
    </section><div className="section-note"><Radio size={14} />Presence comes from job polls. Agents are offline after twice the server poll timeout plus 60 seconds without a poll.</div>
    {agent && <Modal title={`Agent ${shortId(agent.uuid)}`} close={() => setSelected(null)}><Badge status={agent.online ? 'online' : 'offline'}>{agent.online ? 'Online' : 'Offline'}</Badge><Details items={[
      ['Agent ID', <code>{agent.uuid}</code>], ['CPU budget', `${agent.cpu_cores} cores`], ['Memory budget', memory(agent.memory_bytes)], ['Active jobs', `${agent.active_jobs} at last poll`], ['First seen', dateTime(agent.first_seen_at)], ['Last seen', dateTime(agent.last_seen_at)], ['Online until', dateTime(agent.online_until)],
    ]} /><p className="muted small-text">Presence reflects recent polling, not container health. Capacity and active job counts are reported by the agent.</p><div className="modal-actions"><button className="button primary" onClick={() => openJobs(agent.uuid)}><Box size={15} />View claimed jobs</button></div></Modal>}
  </>;
}
