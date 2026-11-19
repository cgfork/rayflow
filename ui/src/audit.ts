import { useEffect, useMemo, useState } from 'react';
import { api } from './api';
import type { User } from './Auth';
import type { Graph } from './types';

const userIdPattern = /^[0-9a-f]{64}$/i;

export function useAuditActorName(graphs: Graph[] | undefined, user: User | null) {
  const currentUserId = user?.id;
  const key = useMemo(() => {
    const ids = new Set<string>();
    for (const graph of graphs ?? []) {
      for (const actor of [graph.created_by, graph.updated_by]) {
        if (actor && actor !== currentUserId && userIdPattern.test(actor)) ids.add(actor);
      }
    }
    return [...ids].sort().join(',');
  }, [graphs, currentUserId]);
  const [lookup, setLookup] = useState<{ key: string; names: Record<string, string> }>();

  useEffect(() => {
    if (!key) return;
    const request = new AbortController();
    void api<Record<string, string>>('/users/resolve', {
      method: 'POST',
      body: JSON.stringify({ ids: key.split(',') }),
      signal: request.signal,
    }).then((names) => {
      if (!request.signal.aborted) setLookup({ key, names });
    }).catch(() => {
      // The stored audit ID remains available when name lookup fails.
    });
    return () => request.abort();
  }, [key]);

  const names = lookup?.key === key ? lookup.names : undefined;
  return (actor?: string) => {
    if (!actor) return '—';
    if (user && actor === user.id) return user.name || actor;
    const name = names?.[actor];
    return typeof name === 'string' && name ? name : actor;
  };
}
