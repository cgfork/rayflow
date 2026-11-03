import { useCallback, useEffect, useState } from 'react';
import { Activity, ArrowUpRight, Box, ChevronRight, GitBranch, Layers3, ListTodo, Menu, Radio, Server, Workflow, X } from 'lucide-react';
import Agents from './Agents';
import Orchestration from './Orchestration';
import Runtime from './Runtime';
import Schedule from './Schedule';
import { Toast } from './components';
import type { Section } from './types';

const navigation = [
  { id: 'orchestration', title: 'Orchestration', subtitle: 'Build & publish', icon: Workflow },
  { id: 'runtime', title: 'Runtime', subtitle: 'Workflow execution', icon: Activity },
  { id: 'schedule', title: 'Schedule', subtitle: 'Jobs & claims', icon: ListTodo },
  { id: 'agents', title: 'Agents', subtitle: 'Connected fleet', icon: Server },
] as const;
function currentSection(): Section {
  const section = window.location.hash.replace(/^#\/?/, '');
  return navigation.some((item) => item.id === section) ? section as Section : 'orchestration';
}

export default function App() {
  const [section, setSection] = useState<Section>(currentSection);
  const [actor, setActor] = useState(() => localStorage.getItem('rayflow.actor') || 'operator');
  const [message, setMessage] = useState('');
  const [connected, setConnected] = useState<boolean | null>(null);
  const [menu, setMenu] = useState(false);
  const [dirty, setDirty] = useState(false);
  const [jobFilter, setJobFilter] = useState<{ agent_id?: string; run_id?: string }>({});
  const dismissToast = useCallback(() => setMessage(''), []);
  useEffect(() => { localStorage.setItem('rayflow.actor', actor); }, [actor]);
  useEffect(() => {
    function change() {
      const next = currentSection();
      if (next !== section && dirty && !window.confirm('Discard unsaved workflow changes?')) {
        history.replaceState(null, '', `#/${section}`); return;
      }
      setSection(next); setMenu(false);
    }
    window.addEventListener('hashchange', change);
    return () => window.removeEventListener('hashchange', change);
  }, [dirty, section]);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function check() {
      try {
        const response = await fetch('/api/health', { signal: controller.signal });
        const body = await response.text();
        if (!controller.signal.aborted) setConnected(response.ok && body.trim() === 'ok');
      } catch { if (!controller.signal.aborted) setConnected(false); }
      if (!controller.signal.aborted) timer = setTimeout(check, 15000);
    }
    void check();
    return () => { controller.abort(); clearTimeout(timer); };
  }, []);
  function navigate(next: Section) { window.location.hash = `/${next}`; setMenu(false); }
  const active = navigation.find((item) => item.id === section)!;
  return <div className="app-shell">
    {menu && <button className="sidebar-overlay" aria-label="Close navigation" onClick={() => setMenu(false)} />}
    <aside className={`sidebar ${menu ? 'open' : ''}`}><a className="brand" href="#/orchestration"><span className="brand-mark"><GitBranch size={25} /></span><span>rayflow<span className="brand-period">.</span></span><small>CONSOLE</small></a><button className="mobile-close icon-button" aria-label="Close navigation" onClick={() => setMenu(false)}><X size={20} /></button>
      <div className="workspace-switch"><span className="workspace-symbol"><Layers3 size={17} /></span><div><strong>Rayflow workspace</strong><small>Workflow management</small></div></div>
      <div className="nav-label">WORKSPACE</div><nav>{navigation.map((item) => <a key={item.id} href={`#/${item.id}`} className={`nav-item ${section === item.id ? 'active' : ''}`} aria-current={section === item.id ? 'page' : undefined}><item.icon size={19} /><span>{item.title}<small>{item.subtitle}</small></span>{section === item.id && <ChevronRight size={15} />}</a>)}</nav>
      <div className="sidebar-bottom"><div className="sidebar-about"><div className="orbit"><Box size={21} /></div><strong>Connected by design.</strong><p>From graph to execution,<br />one place to keep work flowing.</p></div><div className="operator"><span className="avatar">{(actor || 'OP').slice(0, 2).toUpperCase()}</span><label>Audit identity<input aria-label="Audit identity" value={actor} maxLength={64} placeholder="operator" onChange={(event) => setActor(event.target.value)} /></label></div></div>
    </aside>
    <div className="main-shell"><header className="topbar"><div className="breadcrumb"><button className="icon-button menu-button" aria-label="Open navigation" onClick={() => setMenu(true)}><Menu size={20} /></button><span>Workspace</span><ChevronRight size={14} /><strong>{active.title}</strong></div><div className="topbar-right"><span className="environment"><span />Default workspace</span><span className={`connection-state ${connected === false ? 'disconnected' : ''}`}><Radio size={14} />{connected === null ? 'Connecting…' : connected ? 'API connected' : 'API unavailable'}</span></div></header>
      <main key={section}>
        {section === 'orchestration' && <Orchestration actor={actor || 'operator'} notify={setMessage} dirtyChanged={setDirty} />}
        {section === 'runtime' && <Runtime notify={setMessage} openJobs={(id) => { setJobFilter({ run_id: id }); navigate('schedule'); }} />}
        {section === 'schedule' && <Schedule notify={setMessage} filter={jobFilter} clearFilter={() => setJobFilter({})} />}
        {section === 'agents' && <Agents openJobs={(id) => { setJobFilter({ agent_id: id }); navigate('schedule'); }} />}
      </main><footer className="page-footer"><span><GitBranch size={13} />Rayflow <span className="footer-divider">/</span> Workflow console</span><span>Built for work that flows<ArrowUpRight size={12} /></span></footer>
    </div>{message && <Toast message={message} close={dismissToast} />}
  </div>;
}
