import { useCallback, useEffect, useState } from 'react';
import type { Page } from './types';

export class ApiError extends Error {
  constructor(public status: number, message: string) { super(message); }
}

export async function api<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(`/api${path}`, {
    ...options,
    headers: { ...(options.body ? { 'Content-Type': 'application/json' } : {}), ...options.headers },
  });
  if (!response.ok) {
    const text = await response.text();
    let message = text || `Request failed (${response.status})`;
    try { message = JSON.parse(text).error || message; } catch { message = text || message; }
    throw new ApiError(response.status, message);
  }
  if (response.status === 204) return undefined as T;
  return response.json();
}

export function send<T>(path: string, body: unknown, method = 'POST') {
  return api<T>(path, { method, body: JSON.stringify(body) });
}

export function useRemote<T>(path: string | null, interval = 0) {
  const [result, setResult] = useState<{ path: string | null; data?: T; error?: string }>({ path });
  const [loading, setLoading] = useState(true);
  const [revision, setRevision] = useState(0);
  const refresh = useCallback(() => setRevision((value) => value + 1), []);
  useEffect(() => {
    if (!path) { setLoading(false); return; }
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function load() {
      setLoading(true);
      try {
        const data = await api<T>(path!, { signal: controller.signal });
        if (!controller.signal.aborted) setResult({ path, data });
      } catch (error) {
        if (!controller.signal.aborted) setResult((previous) => ({
          path, data: previous.path === path ? previous.data : undefined,
          error: error instanceof Error ? error.message : 'Request failed',
        }));
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
          if (interval) timer = setTimeout(tick, interval);
        }
      }
    }
    function tick() {
      if (document.hidden) timer = setTimeout(tick, interval);
      else void load();
    }
    void load();
    return () => { controller.abort(); clearTimeout(timer); };
  }, [path, interval, revision]);
  return { data: result.path === path ? result.data : undefined, error: result.path === path ? result.error : undefined, loading, refresh };
}

export function usePaged<T>(path: string, interval = 0) {
  const [navigation, setNavigation] = useState<{ path: string; cursors: (number | undefined)[] }>({ path, cursors: [undefined] });
  const cursors = navigation.path === path ? navigation.cursors : [undefined];
  const before = cursors[cursors.length - 1];
  const result = useRemote<Page<T>>(`${path}${path.includes('?') ? '&' : '?'}limit=50${before === undefined ? '' : `&before=${before}`}`, interval);
  return { ...result, page: cursors.length,
    next: () => { if (result.data?.next_cursor != null) setNavigation({ path, cursors: [...cursors, result.data.next_cursor] }); },
    previous: () => setNavigation({ path, cursors: cursors.slice(0, -1) }),
  };
}

export const shortId = (id: string) => id.slice(0, 8);
export const dateTime = (value?: string | null) => value ? new Date(value).toLocaleString() : '—';
export function relativeTime(value: string) {
  const seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000));
  if (seconds < 60) return 'Just now';
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return new Date(value).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}
export function memory(bytes: number) {
  if (bytes === 0) return '0 B';
  const index = Math.min(3, Math.floor(Math.log(bytes) / Math.log(1024)));
  return `${Number((bytes / 1024 ** index).toFixed(1))} ${['B', 'KiB', 'MiB', 'GiB'][index]}`;
}
