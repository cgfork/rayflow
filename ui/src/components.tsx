import { useCallback, useEffect, useId, useLayoutEffect, useRef, useState, type ReactNode, type RefObject } from 'react';
import { createPortal } from 'react-dom';
import { AlertCircle, ArrowLeft, ArrowRight, Check, ChevronRight, CircleDashed, Copy, RefreshCw, Search, X } from 'lucide-react';
import { shortId } from './api';
import { copyUuid } from './uuid';

export function UuidText({ id, className }: { id: string; className?: string }) {
  return <code className={className} title={id}>{shortId(id)}</code>;
}

export function UuidCopy({ id, label, notify, children }: {
  id: string; label: string; notify: (message: string) => void; children: ReactNode;
}) {
  const [showTooltip, setShowTooltip] = useState(false);
  const [copied, setCopied] = useState(false);
  const [manualCopy, setManualCopy] = useState(false);
  const anchor = useRef<HTMLSpanElement>(null);
  const tooltip = useRef<HTMLSpanElement>(null);
  const manualCopyInput = useRef<HTMLInputElement>(null);
  const hovering = useRef(false);
  const dismissTimer = useRef<number | undefined>(undefined);
  const tooltipId = useId();
  const copyLabel = `Copy ${label.toLowerCase()} UUID`;

  useLayoutEffect(() => {
    const trigger = anchor.current;
    const tip = tooltip.current;
    if (!showTooltip || !trigger || !tip) return;

    const placeTooltip = () => {
      const bounds = trigger.getBoundingClientRect();
      const { width, height } = tip.getBoundingClientRect();
      const inset = 8;
      const gap = 6;
      const left = Math.max(inset, Math.min(bounds.left, window.innerWidth - width - inset));
      const above = bounds.top - height - gap;
      const below = Math.min(bounds.bottom + gap, window.innerHeight - height - inset);
      tip.style.left = `${left}px`;
      tip.style.top = `${above >= inset ? above : Math.max(inset, below)}px`;
    };
    function dismissWithEscape(event: KeyboardEvent) {
      if (event.key !== 'Escape') return;
      window.clearTimeout(dismissTimer.current);
      hovering.current = false;
      setShowTooltip(false);
    }
    placeTooltip();
    window.addEventListener('resize', placeTooltip);
    window.addEventListener('scroll', placeTooltip, true);
    window.addEventListener('keydown', dismissWithEscape);
    return () => {
      window.removeEventListener('resize', placeTooltip);
      window.removeEventListener('scroll', placeTooltip, true);
      window.removeEventListener('keydown', dismissWithEscape);
    };
  }, [showTooltip, id]);

  useEffect(() => {
    if (!copied) return;
    const timer = window.setTimeout(() => setCopied(false), 1800);
    return () => window.clearTimeout(timer);
  }, [copied]);
  const selectManualCopy = useCallback(() => {
    manualCopyInput.current?.focus();
    manualCopyInput.current?.select();
  }, []);
  useEffect(() => () => window.clearTimeout(dismissTimer.current), []);

  function openTooltip() {
    window.clearTimeout(dismissTimer.current);
    setShowTooltip(true);
  }
  function enterTooltip() {
    hovering.current = true;
    openTooltip();
  }
  function leaveTooltip() {
    hovering.current = false;
    window.clearTimeout(dismissTimer.current);
    dismissTimer.current = window.setTimeout(() => {
      const active = document.activeElement;
      if (active?.matches(':focus-visible') && anchor.current?.contains(active)) return;
      setShowTooltip(false);
    }, 180);
  }

  async function copy() {
    if (await copyUuid(id)) {
      setCopied(true);
      notify(`${label} UUID copied.`);
    } else {
      window.clearTimeout(dismissTimer.current);
      hovering.current = false;
      setShowTooltip(false);
      setManualCopy(true);
    }
  }

  return <>
    <span
      className="uuid-copy" ref={anchor}
      onPointerEnter={(event) => { if (event.pointerType !== 'touch') enterTooltip(); }}
      onPointerLeave={leaveTooltip}
      onFocus={openTooltip}
      onBlur={(event) => { if (!hovering.current && !event.currentTarget.contains(event.relatedTarget)) setShowTooltip(false); }}
    >
      {children}
      <button className="icon-button uuid-copy-button" type="button" aria-label={copyLabel} aria-describedby={showTooltip ? tooltipId : undefined} onClick={() => void copy()}>
        {copied ? <Check size={14} aria-hidden="true" /> : <Copy size={14} aria-hidden="true" />}
      </button>
    </span>
    {showTooltip && createPortal(<span id={tooltipId} role="tooltip" className="uuid-tooltip" ref={tooltip} onPointerEnter={enterTooltip} onPointerLeave={leaveTooltip}>{id}</span>, document.body)}
    {manualCopy && createPortal(<Modal title={copyLabel} onOpen={selectManualCopy} close={() => setManualCopy(false)}>
      <p>Select and copy the full UUID.</p>
      <label className="field">{label} UUID<input ref={manualCopyInput} value={id} readOnly /></label>
    </Modal>, document.body)}
  </>;
}

export function Badge({ children, status = '' }: { children: ReactNode; status?: string }) {
  return <span className={`badge badge-${status.toLowerCase()}`}><i />{children}</span>;
}
export function ErrorBanner({ error, retry }: { error?: string; retry?: () => void }) {
  if (!error) return null;
  return <div className="error-banner" role="alert"><AlertCircle size={17} aria-hidden="true" /><span>{error}</span>{retry && <button className="text-button" type="button" onClick={retry}>Try again</button>}</div>;
}
export function Empty({ title, children, action }: { title: string; children: ReactNode; action?: ReactNode }) {
  return <div className="empty"><div className="empty-icon" aria-hidden="true"><CircleDashed size={30} /></div><h3>{title}</h3><p>{children}</p>{action}</div>;
}
export function Loading({ label = 'Loading workspace…' }: { label?: string }) {
  return <div className="loading" role="status" aria-label={label} aria-live="polite">
    <div className="skeleton" aria-hidden="true">
      <div className="skeleton-header">
        <span className="skeleton-icon" />
        <div className="skeleton-lines"><span className="skeleton-line" /><span className="skeleton-line short" /></div>
      </div>
      <div className="skeleton-row"><span className="skeleton-line" /><span className="skeleton-line short" /></div>
      <div className="skeleton-row"><span className="skeleton-line" /><span className="skeleton-line short" /></div>
    </div>
    <span className="loading-label">{label}</span>
  </div>;
}
export function Refresh({ onClick, loading }: { onClick: () => void; loading: boolean }) {
  return <button className="button" onClick={onClick} disabled={loading}><RefreshCw size={15} className={loading ? 'spin' : ''} />Refresh</button>;
}
export function SearchBox({ value, onChange, placeholder = 'Search this page…' }: { value: string; onChange: (value: string) => void; placeholder?: string }) {
  return <label className="search"><Search size={16} /><input aria-label={placeholder} placeholder={placeholder} value={value} onChange={(event) => onChange(event.target.value)} /></label>;
}
export function PageHeader({ eyebrow, title, description, children }: { eyebrow: ReactNode; title: string; description: string; children?: ReactNode }) {
  return <div className="page-heading"><div><div className="eyebrow">{eyebrow}</div><h1>{title}</h1><p>{description}</p></div><div className="actions">{children}</div></div>;
}
export function Pagination({ page, count, more, previous, next }: { page: number; count: number; more: boolean; previous: () => void; next: () => void }) {
  return <div className="pagination"><span>{count} records on this page</span><div><button className="icon-button" type="button" aria-label="Previous page" disabled={page <= 1} onClick={previous}><ArrowLeft size={16} /></button><span>Page {page}</span><button className="icon-button" type="button" aria-label="Next page" disabled={!more} onClick={next}><ArrowRight size={16} /></button></div></div>;
}
function trackDrawerPosition(dialog: HTMLDialogElement, anchor: HTMLElement) {
  const viewport = document.documentElement;
  function place() {
    const bounds = anchor.getBoundingClientRect();
    const height = Math.min(bounds.height, viewport.clientHeight);
    const top = Math.max(0, Math.min(bounds.top, viewport.clientHeight - height));
    const right = Math.max(0, viewport.clientWidth - bounds.right);
    dialog.style.setProperty('--drawer-top', `${top}px`);
    dialog.style.setProperty('--drawer-right', `${right}px`);
    dialog.style.setProperty('--drawer-height', `${height}px`);
  }
  function revealAndPlace() {
    const bounds = anchor.getBoundingClientRect();
    if (bounds.top < 0 || bounds.bottom > viewport.clientHeight) {
      anchor.scrollIntoView({ block: 'nearest', inline: 'nearest', behavior: 'instant' });
    }
    place();
  }
  revealAndPlace();
  const observer = new ResizeObserver(revealAndPlace);
  observer.observe(anchor);
  window.addEventListener('resize', revealAndPlace);
  window.addEventListener('scroll', place, true);
  return () => {
    observer.disconnect();
    window.removeEventListener('resize', revealAndPlace);
    window.removeEventListener('scroll', place, true);
  };
}

export function Modal({ title, titleTooltip, children, close, onOpen, drawerAnchor, wide = false, presentation = 'modal' }: {
  title: string; titleTooltip?: string; children: ReactNode; close: () => void; onOpen?: () => void;
  drawerAnchor?: RefObject<HTMLElement | null>; wide?: boolean; presentation?: 'modal' | 'drawer';
}) {
  const dialog = useRef<HTMLDialogElement>(null);
  const drawer = presentation === 'drawer';
  useEffect(() => {
    const element = dialog.current;
    if (!element) return;
    const anchor = drawer && drawerAnchor?.current;
    const stopTracking = anchor ? trackDrawerPosition(element, anchor) : undefined;
    element.showModal();
    onOpen?.();
    return () => {
      stopTracking?.();
      element.close();
    };
  }, [drawer, drawerAnchor, onOpen]);
  function dismiss() {
    if (drawer) dialog.current?.close();
    close();
  }
  return <dialog
    ref={dialog}
    className={drawer ? `drawer${drawerAnchor ? ' drawer-anchored' : ''}` : `modal ${wide ? 'wide' : ''}`}
    onCancel={(event) => { event.preventDefault(); dismiss(); }}
    onClick={(event) => {
      if (event.target !== event.currentTarget) return;
      if (drawer) {
        const { left, right, top, bottom } = event.currentTarget.getBoundingClientRect();
        if (event.clientX >= left && event.clientX < right && event.clientY >= top && event.clientY < bottom) return;
      }
      dismiss();
    }}
    aria-label={title}
  >
    <header className={drawer ? 'drawer-header' : undefined}><h2 title={titleTooltip}>{title}</h2><button className="icon-button" type="button" aria-label="Close dialog" onClick={dismiss}><X size={19} /></button></header>
    {drawer ? <div className="drawer-body">{children}</div> : children}
  </dialog>;
}
export function Details({ items }: { items: [string, ReactNode][] }) {
  return <dl className="details">{items.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value}</dd></div>)}</dl>;
}
export function Toast({ message, close }: { message: string; close: () => void }) {
  useEffect(() => { const timer = setTimeout(close, 6000); return () => clearTimeout(timer); }, [message, close]);
  return <div className="toast" role="status"><Check size={18} /><span>{message}</span><button aria-label="Dismiss message" onClick={close}><X size={15} /></button></div>;
}
export function Back({ onClick, children }: { onClick: () => void; children: ReactNode }) {
  return <button className="back-link" onClick={onClick}><ArrowLeft size={14} />{children}<ChevronRight size={14} /></button>;
}
