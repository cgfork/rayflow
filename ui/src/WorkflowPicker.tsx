import { useEffect, useId, useRef, useState } from 'react';
import { Check, ChevronDown, Loader2, Search, Workflow, X } from 'lucide-react';
import { api } from './api';
import { ErrorBanner, UuidText } from './components';
import type { Graph, Page } from './types';
import './WorkflowPicker.css';

export default function WorkflowPicker({ value, onChange }: {
  value: Pick<Graph, 'id' | 'name'> | null;
  onChange: (graph: Graph | null) => void;
}) {
  const root = useRef<HTMLDivElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const id = useId();
  const [items, setItems] = useState<Graph[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();
  const [attempt, setAttempt] = useState(0);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);
  const search = query.trim().toLowerCase();
  const choices = items.filter((graph) => `${graph.name} ${graph.id}`.toLowerCase().includes(search));
  const index = Math.min(active, Math.max(0, choices.length - 1));

  useEffect(() => {
    const controller = new AbortController();
    async function load() {
      setLoading(true); setError(undefined);
      const workflows = new Map<string, Graph>();
      let before: number | null = null;
      try {
        do {
          const page: Page<Graph> = await api(`/graphs?limit=200${before === null ? '' : `&before=${before}`}`, { signal: controller.signal });
          if (controller.signal.aborted) return;
          for (const graph of page.items) {
            const previous = workflows.get(graph.id);
            if (!previous || previous.version < graph.version) workflows.set(graph.id, graph);
          }
          setItems([...workflows.values()].sort((left, right) => left.name.localeCompare(right.name) || left.id.localeCompare(right.id)));
          before = page.next_cursor;
        } while (before !== null);
      } catch (failure) {
        if (!controller.signal.aborted) setError((failure as Error).message);
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    }
    const timer = setTimeout(() => void load(), 0);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [attempt]);

  useEffect(() => {
    if (!open) return;
    function outside(event: PointerEvent) {
      if (!root.current?.contains(event.target as Node)) { setOpen(false); setQuery(''); }
    }
    document.addEventListener('pointerdown', outside);
    return () => document.removeEventListener('pointerdown', outside);
  }, [open]);

  const activeId = choices[index]?.id;
  useEffect(() => {
    if (open && activeId) document.getElementById(`${id}-${activeId}`)?.scrollIntoView({ block: 'nearest' });
  }, [open, activeId, id]);

  function select(graph: Graph) {
    onChange(graph); setQuery(''); setActive(0); setOpen(false);
    input.current?.focus();
  }

  return <div className="workflow-picker" ref={root} onBlurCapture={(event) => {
    if (!event.currentTarget.contains(event.relatedTarget)) { setOpen(false); setQuery(''); }
  }}>
    <div className="workflow-picker-control" onClick={() => { input.current?.focus(); setOpen(true); }}>
      <Search size={16} aria-hidden="true" />
      {value && <span className="workflow-picker-selected" title={`${value.name} · ${value.id}`}>
        <Workflow size={13} aria-hidden="true" /><span>{value.name}</span>
        <button type="button" aria-label="Clear selected workflow" onClick={() => { onChange(null); setQuery(''); setActive(0); }}><X size={12} /></button>
      </span>}
      <input id={id} ref={input} role="combobox" aria-label="Search workflows" aria-expanded={open} aria-controls={`${id}-options`} aria-autocomplete="list"
        aria-activedescendant={open && activeId ? `${id}-${activeId}` : undefined}
        autoComplete="off" placeholder={value ? 'Change workflow…' : 'Search workflows…'} value={query}
        onFocus={() => setOpen(true)} onChange={(event) => { setQuery(event.target.value); setActive(0); setOpen(true); }}
        onKeyDown={(event) => {
          if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
            event.preventDefault(); setOpen(true);
            const last = Math.max(0, choices.length - 1);
            setActive(event.key === 'ArrowDown' ? open ? Math.min(index + 1, last) : 0 : open ? Math.max(0, index - 1) : last);
          } else if (event.key === 'Enter') {
            event.preventDefault(); if (open && choices[index]) select(choices[index]); else setOpen(true);
          } else if (event.key === 'Escape') {
            event.preventDefault(); setOpen(false); setQuery('');
          }
        }} />
      <ChevronDown size={15} aria-hidden="true" />
    </div>
    {open && <div className="workflow-picker-menu">
      <div className="workflow-picker-caption">Published workflows <span>{choices.length}</span></div>
      <ErrorBanner error={error} retry={() => setAttempt((current) => current + 1)} />
      <div id={`${id}-options`} role="listbox" aria-label="Published workflows" className="workflow-picker-options">
        {choices.map((graph, offset) => <button type="button" role="option" tabIndex={-1} id={`${id}-${graph.id}`} key={graph.id}
          aria-label={`${graph.name} ${graph.id}`} aria-selected={graph.id === value?.id} className={`workflow-picker-option ${offset === index ? 'active' : ''}`}
          onMouseDown={(event) => event.preventDefault()} onMouseEnter={() => setActive(offset)} onClick={() => select(graph)}>
          <Workflow size={16} aria-hidden="true" /><span><strong title={graph.name}>{graph.name}</strong><small><UuidText id={graph.id} /></small></span>
          {graph.id === value?.id && <Check size={16} aria-hidden="true" />}
        </button>)}
      </div>
      {loading ? <div className="workflow-picker-message" role="status"><Loader2 size={14} className="spin" />Loading workflows…</div>
        : !choices.length && !error && <div className="workflow-picker-message">{search ? 'No matching workflows.' : 'No published workflows yet.'}</div>}
    </div>}
  </div>;
}
