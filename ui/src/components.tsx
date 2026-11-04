import { useEffect, useRef, type ReactNode } from 'react';
import { AlertCircle, ArrowLeft, ArrowRight, Check, ChevronRight, CircleDashed, Loader2, RefreshCw, Search, X } from 'lucide-react';

export function Badge({ children, status = '' }: { children: ReactNode; status?: string }) {
  return <span className={`badge badge-${status.toLowerCase()}`}><i />{children}</span>;
}
export function ErrorBanner({ error, retry }: { error?: string; retry?: () => void }) {
  if (!error) return null;
  return <div className="error-banner" role="alert"><AlertCircle size={17} /><span>{error}</span>{retry && <button className="text-button" type="button" onClick={retry}>Try again</button>}</div>;
}
export function Empty({ title, children, action }: { title: string; children: ReactNode; action?: ReactNode }) {
  return <div className="empty"><div className="empty-icon"><CircleDashed size={30} /></div><h3>{title}</h3><p>{children}</p>{action}</div>;
}
export function Loading() { return <div className="loading" role="status"><Loader2 className="spin" size={20} />Loading workspace…</div>; }
export function Refresh({ onClick, loading }: { onClick: () => void; loading: boolean }) {
  return <button className="button" onClick={onClick} disabled={loading}><RefreshCw size={15} className={loading ? 'spin' : ''} />Refresh</button>;
}
export function SearchBox({ value, onChange, placeholder = 'Search this page…' }: { value: string; onChange: (value: string) => void; placeholder?: string }) {
  return <label className="search"><Search size={16} /><input aria-label={placeholder} placeholder={placeholder} value={value} onChange={(event) => onChange(event.target.value)} /></label>;
}
export function PageHeader({ eyebrow, title, description, children }: { eyebrow: string; title: string; description: string; children?: ReactNode }) {
  return <div className="page-heading"><div><div className="eyebrow">{eyebrow}</div><h1>{title}</h1><p>{description}</p></div><div className="actions">{children}</div></div>;
}
export function Pagination({ page, count, more, previous, next }: { page: number; count: number; more: boolean; previous: () => void; next: () => void }) {
  return <div className="pagination"><span>{count} records on this page</span><div><button className="icon-button" type="button" aria-label="Previous page" disabled={page <= 1} onClick={previous}><ArrowLeft size={16} /></button><span>Page {page}</span><button className="icon-button" type="button" aria-label="Next page" disabled={!more} onClick={next}><ArrowRight size={16} /></button></div></div>;
}
export function Modal({ title, children, close, wide = false }: { title: string; children: ReactNode; close: () => void; wide?: boolean }) {
  const dialog = useRef<HTMLDialogElement>(null);
  useEffect(() => { const element = dialog.current; element?.showModal(); return () => element?.close(); }, []);
  return <dialog ref={dialog} className={`modal ${wide ? 'wide' : ''}`} onCancel={(event) => { event.preventDefault(); close(); }} onClick={(event) => { if (event.target === dialog.current) close(); }} aria-label={title}>
    <header><h2>{title}</h2><button className="icon-button" aria-label="Close dialog" onClick={close}><X size={19} /></button></header>{children}
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
