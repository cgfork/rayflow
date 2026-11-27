import type { ReactNode } from 'react';
import { api, dateTime, isHttpDownloadUrl, memory, useRemote } from './api';
import { Details, ErrorBanner } from './components';
import type { RunArchiveTransfer } from './types';

interface UsageSummary {
  sampled_at: string;
  total_cpu_usage_ns?: number;
  peak_cpu_millicores?: number;
  peak_memory_usage_bytes?: number;
  peak_memory_working_set_bytes?: number;
  memory_limit_bytes?: number;
}

interface JobResult {
  exit_code: number;
  outcome?: string;
  duration_ms?: number;
  finished_at?: string;
  usage?: UsageSummary;
}

interface JobUsage {
  summary?: UsageSummary;
}

const outcomes = new Map([
  ['succeed', 'Succeeded'], ['succeeded', 'Succeeded'],
  ['fail', 'Failed'], ['failed', 'Failed'],
  ['yield', 'Yielded'], ['yielded', 'Yielded'],
  ['terminate', 'Terminated'], ['terminated', 'Terminated'],
]);

async function downloadArchive<T>(path: string, options: RequestInit): Promise<T | null> {
  const signed = await api<RunArchiveTransfer | null>(path, options);
  if (signed === null) return null;
  if (signed?.operation !== 'download' || !isHttpDownloadUrl(signed.url)) {
    throw new Error('The server returned an invalid archive download link.');
  }
  const response = await fetch(signed.url, {
    headers: signed.headers,
    signal: options.signal,
    credentials: 'omit',
    referrerPolicy: 'no-referrer',
    redirect: 'error',
  });
  if (!response.ok) throw new Error(`Archive download failed (HTTP ${response.status}).`);
  const archive: unknown = await response.json().catch(() => undefined);
  if (!archive || typeof archive !== 'object' || Array.isArray(archive)) {
    throw new Error('Object storage returned an invalid JSON archive.');
  }
  return archive as T;
}

function duration(milliseconds: number) {
  return milliseconds < 1000
    ? `${Number(milliseconds.toFixed(2))} ms`
    : `${Number((milliseconds / 1000).toFixed(2))} s`;
}

function UsageDetails({ summary }: { summary: UsageSummary }) {
  const items: [string, ReactNode][] = [];
  if (summary.total_cpu_usage_ns != null) items.push(['CPU time', duration(summary.total_cpu_usage_ns / 1_000_000)]);
  if (summary.peak_cpu_millicores != null) items.push(['Peak CPU', `${summary.peak_cpu_millicores / 1000} cores`]);
  if (summary.peak_memory_usage_bytes != null) items.push(['Peak memory', memory(summary.peak_memory_usage_bytes)]);
  if (summary.peak_memory_working_set_bytes != null) items.push(['Peak working set', memory(summary.peak_memory_working_set_bytes)]);
  if (summary.memory_limit_bytes != null) items.push(['Memory limit', memory(summary.memory_limit_bytes)]);
  items.push(['Last sample', dateTime(summary.sampled_at)]);
  return <Details items={items} />;
}

export function useNodeArchives(runId: string, nodeRunId: string | undefined) {
  const path = nodeRunId ? `/runs/${runId}/node-runs/${nodeRunId}/artifacts` : null;
  const result = useRemote(path && `${path}/result.json`, downloadArchive<JobResult>);
  const usage = useRemote(path && `${path}/usage.json`, downloadArchive<JobUsage>);
  return { result, usage };
}

export default function RuntimeNodeArtifacts({ result, usage }: ReturnType<typeof useNodeArchives>) {
  const resultData = result.data;
  // Earlier agents stored the usage summary in result.json.
  const summary = usage.data === null ? resultData?.usage : usage.data?.summary;
  return <>
    <section className="field-section runtime-node-artifact" aria-label="Result">
      <h3>Result</h3>
      <ErrorBanner error={result.error} retry={result.refresh} />
      {resultData ? <Details items={[
        ['Outcome', resultData.outcome
          ? outcomes.get(resultData.outcome) ?? resultData.outcome
          : resultData.exit_code === 0 ? 'Succeeded' : 'Failed'],
        ['Exit code', resultData.exit_code],
        ['Duration', resultData.duration_ms != null ? duration(resultData.duration_ms) : '—'],
        ['Finished', dateTime(resultData.finished_at)],
      ]} /> : !result.error && <p>{resultData === null ? 'Result has not been archived.' : 'Loading result…'}</p>}
    </section>
    <section className="field-section runtime-node-artifact" aria-label="Usage">
      <h3>Usage</h3>
      <ErrorBanner error={usage.error} retry={usage.refresh} />
      {summary ? <UsageDetails summary={summary} /> : !usage.error && <p>{usage.data
        ? 'No usage samples were recorded.'
        : usage.data === null ? 'Usage has not been archived.' : 'Loading usage…'}</p>}
    </section>
  </>;
}
