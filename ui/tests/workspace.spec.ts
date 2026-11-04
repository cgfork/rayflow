import { expect, test, type Page as BrowserPage } from '@playwright/test';
import type { Graph, Job, NodeStatus, RunView } from '../src/types';

const graphId = '10000000-0000-4000-8000-000000000001';
const runId = '20000000-0000-4000-8000-000000000001';
const dataId = '30000000-0000-4000-8000-000000000001';
const agentId = '40000000-0000-4000-8000-000000000001';
const jobId = '50000000-0000-4000-8000-000000000001';
const now = new Date().toISOString();
const graph: Graph = {
  id: graphId, name: 'daily-data-pipeline', version: 2, node_count: 4,
  nodes: ['extract_data', 'clean_records', 'validate_schema', 'publish_dataset'].map((name, index) => ({
    id: `60000000-0000-4000-8000-00000000000${index + 1}`, name, node_type: 'docker', inputs: [], outputs: [],
    settings: { config: { image: 'alpine:3.21' }, resource: { cpu_cores: 1, memory_bytes: 268435456 }, timeout: '30s' },
  })),
  edges: [[1, 2], [1, 3], [2, 4], [3, 4]].map(([source, target]) => [
    `60000000-0000-4000-8000-00000000000${source}`, `60000000-0000-4000-8000-00000000000${target}`,
  ]),
};
const baseJob: Job = { id: 1, uuid: jobId, task_id: '70000000-0000-4000-8000-000000000001', run_id: runId, node_run_id: 1, status: 'Polled', required_cpu_cores: 2, required_memory_bytes: 2147483648, agent_id: agentId, polled_at: now, expired_at: new Date(Date.now() + 60000).toISOString(), created_at: now, updated_at: now };

async function backend(page: BrowserPage, options: { empty?: boolean; historical?: boolean; rootStatus?: NodeStatus; rootRevision?: number; versions?: number[]; sharedNodes?: Record<number, string[]> } = {}) {
  let draft = structuredClone(graph);
  let jobs = [structuredClone(baseJob)];
  const calls: { path: string; method: string; body: unknown }[] = [];
  const failures = new Map<string, number>();
  await page.route('https://fonts.googleapis.com/**', (route) => route.abort());
  await page.route('**/api/**', async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname.replace('/api', '');
    const method = request.method();
    const body = request.postDataJSON() as Record<string, unknown> | null;
    calls.push({ path, method, body });
    const fulfill = (data: unknown, status = 200) => route.fulfill({ status, json: data });
    if (failures.has(path)) return fulfill({ error: 'Database temporarily unavailable' }, failures.get(path));
    if (path === '/health') return route.fulfill({ status: 200, body: 'ok' });
    if (path === '/graph_drafts') return fulfill({ items: options.empty ? [] : [draft], next_cursor: null });
    if (path === '/graphs') return fulfill({ items: options.empty ? [] : (options.versions ?? [graph.version]).map((version) => ({ ...graph, version })), next_cursor: null });
    if (path.endsWith('/draft')) {
      if (method === 'PUT') {
        draft = body!.graph as Graph;
        draft.node_count = draft.nodes.length;
      }
      return fulfill(draft);
    }
    if (path.endsWith('/publish')) return fulfill({ ...draft, version: 3 }, 201);
    if (path === `/graphs/${graphId}`) return fulfill(graph);
    if (path === `/graphs/${graphId}/shared-nodes`) return fulfill(options.sharedNodes?.[Number(url.searchParams.get('version'))] ?? [graph.nodes[0].id]);
    if (path === '/runs/start' || path === '/runs/resume' || path === '/runs/rerun' || path === '/runs/upgrade') return fulfill({ id: crypto.randomUUID() }, 202);
    if (path === '/runs') return fulfill({ items: [{ id: 1, uuid: runId, graph_id: graphId, data_id: dataId, version: 2, revision: 3, created_at: now, updated_at: now }], next_cursor: null });
    if (path === `/runs/${runId}`) {
      const result: RunView = { graph, current: !options.historical, state: { uuid: runId, graph_id: graphId, data_id: dataId, version: 2, revision: 3,
        nodes: graph.nodes.slice(0, 3).map((node, index) => ({ ...node, node_id: node.id, status: index === 0 ? options.rootStatus ?? 'Succeeded' : index === 1 ? 'Pending' : 'Running', revision: index === 0 ? options.rootRevision ?? 3 : 3, updated_at: now })),
      } };
      return fulfill(result);
    }
    if (path === '/jobs') return fulfill({ items: jobs.filter((job) => !url.searchParams.has('status') || job.status === url.searchParams.get('status')), next_cursor: null });
    if (path === '/job/cancel' || path === '/job/terminate') {
      jobs = jobs.map((job) => ({ ...job, status: 'Terminated', agent_id: null }));
      return route.fulfill({ status: 204 });
    }
    if (path === '/agents') {
      const agents = [{ id: 1, uuid: agentId, cpu_cores: 8, memory_bytes: 17179869184, active_jobs: 2, first_seen_at: now, last_seen_at: now, online_until: new Date(Date.now() + 120000).toISOString(), online: true },
        { id: 2, uuid: '40000000-0000-4000-8000-000000000002', cpu_cores: 4, memory_bytes: 8589934592, active_jobs: 0, first_seen_at: now, last_seen_at: now, online_until: now, online: false }];
      return fulfill({ items: agents.filter((agent) => !url.searchParams.has('online') || String(agent.online) === url.searchParams.get('online')), next_cursor: null });
    }
    return fulfill({ error: `Unexpected API call: ${path}` }, 404);
  });
  return { calls, failures };
}

test('builds a graph, saves its draft, and publishes through the existing APIs', async ({ page }) => {
  const { calls } = await backend(page, { empty: true });
  await page.goto('/');
  await expect(page.getByText('Your next workflow starts here')).toBeVisible();
  await page.getByRole('button', { name: 'New workflow' }).click();
  await page.getByLabel('Workflow name', { exact: true }).fill('nightly-import');
  await page.getByRole('button', { name: 'Create workflow', exact: true }).click();
  await page.getByLabel('Node name', { exact: true }).fill('extract');
  await page.getByLabel('Docker image', { exact: true }).fill('alpine:3.21');
  await page.getByRole('button', { name: 'Add node', exact: true }).click();
  await page.getByLabel('Docker image', { exact: true }).fill('alpine:3.21');
  await expect(page.getByRole('button', { name: 'Publish', exact: true })).toBeDisabled();
  await page.locator('.react-flow__node').filter({ hasText: 'extract' }).click();
  await page.getByLabel('Connect to node').selectOption({ label: 'task_2' });
  await page.getByRole('button', { name: 'Add connection', exact: true }).click();
  await expect(page.getByText('Ready to publish', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Save draft' }).click();
  await expect(page.getByText('All changes saved')).toBeVisible();
  const saved = calls.find((call) => call.method === 'PUT')!.body as { graph: Graph; actor: string };
  expect(saved.graph.nodes).toHaveLength(2);
  expect(saved.graph.edges).toEqual([[saved.graph.nodes[0].id, saved.graph.nodes[1].id]]);
  expect(saved.actor).toBe('operator');
  await page.getByRole('button', { name: 'Publish', exact: true }).click();
  await page.getByRole('button', { name: 'Publish version' }).click();
  await expect(page.getByRole('status').filter({ hasText: 'Published nightly-import' })).toBeVisible();
  expect(calls.some((call) => call.path.endsWith('/publish') && call.method === 'POST')).toBe(true);
});

test('validates cycles, edits JSON, and preserves unsaved work when navigation is cancelled', async ({ page }) => {
  const { calls } = await backend(page);
  await page.goto('/');
  await page.getByRole('button', { name: 'Edit daily-data-pipeline' }).click();
  await expect(page.getByLabel('Node name')).toHaveValue('extract_data');
  await page.getByRole('button', { name: 'Edit JSON' }).click();
  const circular = { ...graph, edges: [...graph.edges, [graph.nodes[3].id, graph.nodes[0].id]] };
  await page.getByRole('textbox', { name: 'Workflow JSON', exact: true }).fill(JSON.stringify(circular));
  await page.getByRole('button', { name: 'Apply changes' }).click();
  await expect(page.getByText('Remove circular dependencies before publishing.')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Publish', exact: true })).toBeDisabled();
  page.once('dialog', (dialog) => dialog.dismiss());
  await page.getByRole('link', { name: 'Runtime Workflow execution' }).click();
  await expect(page.getByLabel('Node name')).toHaveValue('extract_data');
  expect(calls.filter((call) => call.path.endsWith('/publish'))).toHaveLength(0);
});

test('shows every runtime node and submits resume without pretending the state changed', async ({ page }) => {
  const { calls } = await backend(page);
  await page.goto('/#/runtime');
  await page.getByRole('button', { name: 'Inspect run 20000000' }).click();
  await expect(page.locator('.react-flow__node')).toHaveCount(4);
  await expect(page.locator('.react-flow__node').filter({ hasText: 'publish_dataset' })).toContainText('Waiting');
  await page.locator('.react-flow__node').filter({ hasText: 'clean_records' }).click();
  await page.getByRole('button', { name: 'Resume node' }).click();
  await page.getByRole('button', { name: 'Confirm command' }).click();
  await expect(page.getByRole('status').filter({ hasText: 'Resume command accepted' })).toBeVisible();
  expect(calls.find((call) => call.path === '/runs/resume')?.body).toEqual({ graph_id: graphId, data_id: dataId, node: graph.nodes[1].id });
  await expect(page.locator('.inspector .badge')).toHaveText('Pending');
});

test('upgrades from the Runtime list using a shared node from an earlier revision', async ({ page }) => {
  const { calls } = await backend(page, { versions: [3, 2, 1], rootRevision: 2 });
  await page.goto('/#/runtime');
  await page.getByRole('button', { name: 'Upgrade run 20000000' }).click();
  const dialog = page.getByRole('dialog', { name: 'Upgrade workflow' });
  const versions = dialog.getByRole('combobox', { name: 'Target version' });
  const nodes = dialog.getByRole('combobox', { name: 'Shared restart node' });
  await expect(dialog.getByRole('button', { name: 'Confirm upgrade' })).toBeDisabled();
  await expect(versions.locator('option')).toHaveCount(2);
  const sharedRequest = page.waitForRequest((request) => request.url().includes('/shared-nodes?'));
  await versions.selectOption('3');
  expect(new URL((await sharedRequest).url()).searchParams.toString()).toBe('from_version=2&version=3');
  await expect(nodes.locator('option')).toHaveCount(2);
  await expect(nodes.locator(`option[value="${graph.nodes[0].id}"]`)).toHaveText('extract_data · Succeeded (earlier revision)');
  await nodes.selectOption(graph.nodes[0].id);
  await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
  expect(calls.some((call) => call.path === '/runs/upgrade')).toBe(false);
  await page.getByRole('button', { name: 'Upgrade run 20000000' }).click();
  await versions.selectOption('3');
  await nodes.selectOption(graph.nodes[0].id);
  await page.screenshot({ path: 'test-results/runtime-upgrade.png', fullPage: true });
  await dialog.getByRole('button', { name: 'Confirm upgrade' }).click();
  await expect(dialog).toHaveCount(0);
  await expect(page.getByRole('status').filter({ hasText: 'Upgrade command accepted for v3 from extract_data' })).toBeVisible();
  expect(calls.filter((call) => call.path === '/runs/upgrade')).toEqual([
    { path: '/runs/upgrade', method: 'POST', body: { graph_id: graphId, data_id: dataId, version: 3, node: graph.nodes[0].id } },
  ]);
  expect(calls.some((call) => call.path === '/runs/start' || call.path === '/runs/rerun')).toBe(false);
  await expect(page.locator('tbody .badge')).toHaveText('v2');
});

test('retries a rejected upgrade from the Runtime detail header', async ({ page }) => {
  const { calls, failures } = await backend(page, { versions: [3, 2] });
  failures.set('/runs/upgrade', 503);
  await page.goto('/#/runtime');
  await page.getByRole('button', { name: 'Inspect run 20000000' }).click();
  await page.getByRole('button', { name: 'Upgrade', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: 'Upgrade workflow' });
  await dialog.getByRole('combobox', { name: 'Target version' }).selectOption('3');
  await dialog.getByRole('combobox', { name: 'Shared restart node' }).selectOption(graph.nodes[0].id);
  await dialog.getByRole('button', { name: 'Confirm upgrade' }).click();
  await expect(dialog.getByRole('alert')).toContainText('Database temporarily unavailable');
  await expect(page.getByRole('status').filter({ hasText: 'Upgrade command accepted' })).toHaveCount(0);
  failures.delete('/runs/upgrade');
  await dialog.getByRole('button', { name: 'Try again' }).click();
  await expect(dialog.getByRole('alert')).toHaveCount(0);
  expect(calls.filter((call) => call.path === '/runs/upgrade')).toHaveLength(1);
  await dialog.getByRole('button', { name: 'Confirm upgrade' }).click();
  await expect(dialog).toHaveCount(0);
  expect(calls.filter((call) => call.path === '/runs/upgrade')).toHaveLength(2);
});

test('blocks upgrades from active and waiting shared nodes', async ({ page }) => {
  const { calls } = await backend(page, { versions: [3, 2], sharedNodes: { 3: graph.nodes.slice(1).map((node) => node.id) } });
  await page.goto('/#/runtime');
  await page.getByRole('button', { name: 'Upgrade run 20000000' }).click();
  const dialog = page.getByRole('dialog', { name: 'Upgrade workflow' });
  await dialog.getByRole('combobox', { name: 'Target version' }).selectOption('3');
  const nodes = dialog.getByRole('combobox', { name: 'Shared restart node' });
  for (const node of graph.nodes.slice(1)) await expect(nodes.locator(`option[value="${node.id}"]`)).toHaveJSProperty('disabled', true);
  await expect(nodes.locator(`option[value="${graph.nodes[0].id}"]`)).toHaveCount(0);
  await expect(dialog.getByText(/No shared nodes are eligible/)).toBeVisible();
  await expect(dialog.getByRole('button', { name: 'Confirm upgrade' })).toBeDisabled();
  expect(calls.some((call) => call.path === '/runs/upgrade')).toBe(false);
});

test('clears the shared node when changing upgrade versions and blocks incompatible versions', async ({ page }) => {
  const { calls } = await backend(page, { versions: [4, 3, 2], sharedNodes: { 3: [graph.nodes[0].id], 4: [] } });
  await page.goto('/#/runtime');
  await page.getByRole('button', { name: 'Upgrade run 20000000' }).click();
  const dialog = page.getByRole('dialog', { name: 'Upgrade workflow' });
  const versions = dialog.getByRole('combobox', { name: 'Target version' });
  const nodes = dialog.getByRole('combobox', { name: 'Shared restart node' });
  await versions.selectOption('3');
  await nodes.selectOption(graph.nodes[0].id);
  await expect(dialog.getByRole('button', { name: 'Confirm upgrade' })).toBeEnabled();
  await versions.selectOption('4');
  await expect(nodes).toHaveValue('');
  await expect(dialog.getByText('These versions have no shared restart nodes.')).toBeVisible();
  await expect(dialog.getByRole('button', { name: 'Confirm upgrade' })).toBeDisabled();
  await versions.selectOption('3');
  await expect(nodes).toHaveValue('');
  await expect(dialog.getByRole('button', { name: 'Confirm upgrade' })).toBeDisabled();
  expect(calls.some((call) => call.path === '/runs/upgrade')).toBe(false);
});

test('handles missing newer versions and historical runs in the upgrade dialog', async ({ page }) => {
  const { calls } = await backend(page, { historical: true });
  await page.goto('/#/runtime');
  await page.getByRole('button', { name: 'Upgrade run 20000000' }).click();
  const dialog = page.getByRole('dialog', { name: 'Upgrade workflow' });
  await expect(dialog.getByText('No newer published versions are available.')).toBeVisible();
  await expect(dialog.getByText(/This is a historical run version/)).toBeVisible();
  await expect(dialog.getByRole('combobox', { name: 'Target version' })).toBeDisabled();
  await expect(dialog.getByRole('button', { name: 'Confirm upgrade' })).toBeDisabled();
  await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
  await page.getByRole('button', { name: 'Inspect run 20000000' }).click();
  await expect(page.getByRole('button', { name: 'Upgrade', exact: true })).toBeDisabled();
  expect(calls.some((call) => call.path === '/runs/upgrade')).toBe(false);
});

test('retries shared-node lookup failures before allowing an upgrade', async ({ page }) => {
  const { failures } = await backend(page, { versions: [3, 2] });
  failures.set(`/graphs/${graphId}/shared-nodes`, 503);
  await page.goto('/#/runtime');
  await page.getByRole('button', { name: 'Upgrade run 20000000' }).click();
  const dialog = page.getByRole('dialog', { name: 'Upgrade workflow' });
  await dialog.getByRole('combobox', { name: 'Target version' }).selectOption('3');
  await expect(dialog.getByRole('alert')).toContainText('Database temporarily unavailable');
  await expect(dialog.getByRole('button', { name: 'Confirm upgrade' })).toBeDisabled();
  failures.delete(`/graphs/${graphId}/shared-nodes`);
  await dialog.getByRole('button', { name: 'Try again' }).click();
  await dialog.getByRole('combobox', { name: 'Shared restart node' }).selectOption(graph.nodes[0].id);
  await expect(dialog.getByRole('button', { name: 'Confirm upgrade' })).toBeEnabled();
});

test('pages through upgrade versions without submitting a selected command', async ({ page }) => {
  const { calls } = await backend(page);
  await page.route('**/api/graphs?**', async (route) => {
    const url = new URL(route.request().url());
    expect(url.searchParams.get('graph_id')).toBe(graphId);
    const older = url.searchParams.get('before') === '50';
    return route.fulfill({ json: { items: [{ ...graph, version: older ? 3 : 4 }], next_cursor: older ? null : 50 } });
  });
  await page.goto('/#/runtime');
  await page.getByRole('button', { name: 'Upgrade run 20000000' }).click();
  const dialog = page.getByRole('dialog', { name: 'Upgrade workflow' });
  const versions = dialog.getByRole('combobox', { name: 'Target version' });
  const nodes = dialog.getByRole('combobox', { name: 'Shared restart node' });
  await versions.selectOption('4');
  await nodes.selectOption(graph.nodes[0].id);
  await dialog.getByRole('button', { name: 'Next page' }).click();
  await expect(versions).toHaveValue('');
  await expect(nodes).toHaveValue('');
  await expect(dialog.getByRole('button', { name: 'Confirm upgrade' })).toBeDisabled();
  expect(calls.some((call) => call.path === '/runs/upgrade')).toBe(false);
  await versions.selectOption('3');
  await nodes.selectOption(graph.nodes[0].id);
  await dialog.getByRole('button', { name: 'Confirm upgrade' }).click();
  await expect(dialog).toHaveCount(0);
  expect(calls.find((call) => call.path === '/runs/upgrade')?.body).toEqual({ graph_id: graphId, data_id: dataId, version: 3, node: graph.nodes[0].id });
});

test('reruns from the Runtime list using the same data and graph IDs', async ({ page }) => {
  const { calls } = await backend(page);
  await page.goto('/#/runtime');
  await page.getByRole('button', { name: 'Rerun run 20000000' }).click();
  const dialog = page.getByRole('dialog', { name: 'Rerun workflow' });
  await expect(dialog.getByRole('button', { name: 'Confirm command' })).toBeDisabled();
  const picker = dialog.getByRole('combobox', { name: 'Start from node' });
  await expect(picker.locator(`option[value="${graph.nodes[1].id}"]`)).toHaveJSProperty('disabled', true);
  await expect(picker.locator(`option[value="${graph.nodes[2].id}"]`)).toHaveJSProperty('disabled', true);
  await expect(picker.locator(`option[value="${graph.nodes[3].id}"]`)).toHaveJSProperty('disabled', true);
  await picker.selectOption(graph.nodes[0].id);
  expect(calls.some((call) => call.path === '/runs/rerun')).toBe(false);
  await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
  expect(calls.some((call) => call.path === '/runs/rerun')).toBe(false);

  await page.getByRole('button', { name: 'Rerun run 20000000' }).click();
  await picker.selectOption(graph.nodes[0].id);
  await page.screenshot({ path: 'test-results/runtime-rerun.png', fullPage: true });
  await dialog.getByRole('button', { name: 'Confirm command' }).click();
  await expect(page.getByRole('status').filter({ hasText: 'Rerun command accepted for extract_data' })).toBeVisible();
  expect(calls.filter((call) => call.path === '/runs/rerun')).toEqual([
    { path: '/runs/rerun', method: 'POST', body: { graph_id: graphId, data_id: dataId, node: graph.nodes[0].id } },
  ]);
  expect(calls.some((call) => call.path === '/runs/start')).toBe(false);
  await page.getByRole('button', { name: 'Inspect run 20000000' }).click();
  await expect(page.getByText('Revision 3', { exact: true })).toBeVisible();
  await expect(page.locator('.react-flow__node').filter({ hasText: 'extract_data' })).toContainText('Succeeded');
});

for (const status of ['Ready', 'Succeeded', 'Failed', 'Terminated'] as const) {
  test(`reruns a ${status} node from the Runtime inspector`, async ({ page }) => {
    const { calls } = await backend(page, { rootStatus: status });
    await page.goto('/#/runtime');
    await page.getByRole('button', { name: 'Inspect run 20000000' }).click();
    await page.locator('.react-flow__node').filter({ hasText: 'extract_data' }).click();
    await page.getByRole('button', { name: 'Rerun node', exact: true }).click();
    await expect(page.getByRole('combobox', { name: 'Start from node' })).toHaveValue(graph.nodes[0].id);
    await page.getByRole('button', { name: 'Confirm command' }).click();
    await expect(page.getByRole('status').filter({ hasText: 'Rerun command accepted' })).toBeVisible();
    expect(calls.find((call) => call.path === '/runs/rerun')?.body).toEqual({ graph_id: graphId, data_id: dataId, node: graph.nodes[0].id });
    await expect(page.locator('.inspector .badge')).toHaveText(status);
  });
}

test('retries a rejected rerun from the run detail header', async ({ page }) => {
  const { calls, failures } = await backend(page);
  failures.set('/runs/rerun', 503);
  await page.goto('/#/runtime');
  await page.getByRole('button', { name: 'Inspect run 20000000' }).click();
  await page.getByRole('button', { name: 'Rerun', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: 'Rerun workflow' });
  await dialog.getByRole('combobox', { name: 'Start from node' }).selectOption(graph.nodes[0].id);
  await dialog.getByRole('button', { name: 'Confirm command' }).click();
  await expect(dialog.getByRole('alert')).toContainText('Database temporarily unavailable');
  await expect(page.getByRole('status').filter({ hasText: 'Rerun command accepted' })).toHaveCount(0);
  failures.delete('/runs/rerun');
  await dialog.getByRole('button', { name: 'Confirm command' }).click();
  await expect(page.getByRole('status').filter({ hasText: 'Rerun command accepted' })).toBeVisible();
  expect(calls.filter((call) => call.path === '/runs/rerun')).toHaveLength(2);
});

test('reruns an earlier-revision node from the Runtime list and inspector', async ({ page }) => {
  const { calls } = await backend(page, { rootRevision: 2 });
  await page.goto('/#/runtime');
  await page.getByRole('button', { name: 'Rerun run 20000000' }).click();
  const dialog = page.getByRole('dialog', { name: 'Rerun workflow' });
  const picker = dialog.getByRole('combobox', { name: 'Start from node' });
  const option = picker.locator(`option[value="${graph.nodes[0].id}"]`);
  await expect(option).toHaveJSProperty('disabled', false);
  await expect(option).toHaveText('extract_data · Succeeded (earlier revision)');
  await picker.selectOption(graph.nodes[0].id);
  await dialog.getByRole('button', { name: 'Confirm command' }).click();
  await expect(dialog).toHaveCount(0);
  await page.getByRole('button', { name: 'Inspect run 20000000' }).click();
  await expect(page.getByRole('button', { name: 'Rerun', exact: true })).toBeEnabled();
  await page.locator('.react-flow__node').filter({ hasText: 'extract_data' }).click();
  await page.getByRole('button', { name: 'Rerun node', exact: true }).click();
  await expect(picker).toHaveValue(graph.nodes[0].id);
  await dialog.getByRole('button', { name: 'Confirm command' }).click();
  await expect(dialog).toHaveCount(0);
  expect(calls.filter((call) => call.path === '/runs/rerun')).toEqual([
    { path: '/runs/rerun', method: 'POST', body: { graph_id: graphId, data_id: dataId, node: graph.nodes[0].id } },
    { path: '/runs/rerun', method: 'POST', body: { graph_id: graphId, data_id: dataId, node: graph.nodes[0].id } },
  ]);
  expect(calls.some((call) => call.path === '/runs/start')).toBe(false);
});

test('blocks reruns for active and waiting nodes including earlier revisions', async ({ page }) => {
  const { calls } = await backend(page, { rootRevision: 2, rootStatus: 'Pending' });
  await page.goto('/#/runtime');
  await page.getByRole('button', { name: 'Rerun run 20000000' }).click();
  const dialog = page.getByRole('dialog', { name: 'Rerun workflow' });
  await expect(dialog.getByText(/No nodes are eligible/)).toBeVisible();
  await expect(dialog.getByRole('button', { name: 'Confirm command' })).toBeDisabled();
  await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
  await page.getByRole('button', { name: 'Inspect run 20000000' }).click();
  await expect(page.getByRole('button', { name: 'Rerun', exact: true })).toBeDisabled();
  for (const node of graph.nodes) {
    await page.locator('.react-flow__node').filter({ hasText: node.name }).click();
    await expect(page.getByRole('button', { name: 'Rerun node', exact: true })).toBeDisabled();
  }
  await page.locator('.react-flow__node').filter({ hasText: 'extract_data' }).click();
  await expect(page.getByRole('button', { name: 'Resume node' })).toBeDisabled();
  expect(calls.some((call) => call.path === '/runs/rerun')).toBe(false);
});

test('keeps historical run commands disabled', async ({ page }) => {
  await backend(page, { historical: true });
  await page.goto('/#/runtime');
  await page.getByRole('button', { name: 'Rerun run 20000000' }).click();
  const dialog = page.getByRole('dialog', { name: 'Rerun workflow' });
  await expect(dialog.getByText(/This is a historical run version/)).toBeVisible();
  await expect(dialog.getByRole('button', { name: 'Confirm command' })).toBeDisabled();
  await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
  await page.getByRole('button', { name: 'Inspect run 20000000' }).click();
  await expect(page.getByRole('button', { name: 'Rerun', exact: true })).toBeDisabled();
  await page.locator('.react-flow__node').filter({ hasText: 'clean_records' }).click();
  await expect(page.getByRole('button', { name: 'Resume node' })).toBeDisabled();
  await expect(page.getByRole('button', { name: 'Rerun node' })).toBeDisabled();
  await expect(page.getByText(/This is a historical run version/)).toBeVisible();
});

test('starts the latest published workflow using a distinct data ID', async ({ page }) => {
  const { calls } = await backend(page);
  await page.goto('/#/runtime');
  await page.getByRole('button', { name: 'Start a run', exact: true }).click();
  await page.getByRole('combobox', { name: 'Published workflow', exact: true }).selectOption(graphId);
  await page.getByRole('button', { name: 'Start run', exact: true }).click();
  await expect(page.getByRole('status').filter({ hasText: 'Start command accepted' })).toBeVisible();
  const body = calls.find((call) => call.path === '/runs/start')?.body as { graph_id: string; data_id: string; nodes: string[] };
  expect(body.graph_id).toBe(graphId);
  expect(body.data_id).toMatch(/^[0-9a-f-]{36}$/);
  expect(body.nodes).toEqual([]);
});

test('starts a workflow by UUID when it is absent from the recent graph list', async ({ page }) => {
  const { calls } = await backend(page, { empty: true });
  await page.goto('/#/runtime');
  await page.getByRole('button', { name: 'Start a run', exact: true }).click();
  await page.getByLabel('Graph ID', { exact: true }).fill(graphId);
  await page.getByRole('button', { name: 'Start run', exact: true }).click();
  await expect(page.getByRole('status').filter({ hasText: 'Start command accepted' })).toBeVisible();
  expect(calls.find((call) => call.path === '/runs/start')?.body).toMatchObject({ graph_id: graphId });
});

test('clears the connection target when selecting another node', async ({ page }) => {
  await backend(page);
  await page.goto('/');
  await page.getByRole('button', { name: /daily-data-pipeline/ }).first().click();
  await page.locator('.react-flow__node').filter({ hasText: 'clean_records' }).click();
  await page.getByLabel('Connect to node').selectOption(graph.nodes[2].id);
  await page.locator('.react-flow__node').filter({ hasText: 'validate_schema' }).click();
  await expect(page.getByLabel('Connect to node')).toHaveValue('');
  await expect(page.getByRole('button', { name: 'Add connection', exact: true })).toBeDisabled();
});

test('filters job states and cancels an owned claim with confirmation', async ({ page }) => {
  const { calls } = await backend(page);
  await page.goto('/#/schedule');
  await page.getByLabel('Filter jobs by status').selectOption('Scheduled');
  await expect(page.getByText('No jobs in this view')).toBeVisible();
  await page.getByLabel('Filter jobs by status').selectOption('Polled');
  await page.getByRole('button', { name: 'Inspect job 50000000' }).click();
  await page.getByRole('button', { name: 'Cancel job' }).click();
  expect(calls.some((call) => call.path === '/job/cancel')).toBe(false);
  await page.getByRole('button', { name: 'Confirm action' }).click();
  await expect(page.getByRole('status').filter({ hasText: 'Job 50000000 terminated' })).toBeVisible();
  expect(calls.find((call) => call.path === '/job/cancel')?.body).toEqual({ job_id: jobId, agent_id: agentId });
});

test('shows online and offline agents and opens their claimed jobs', async ({ page }) => {
  const { calls } = await backend(page);
  await page.goto('/#/agents');
  await expect(page.locator('tbody .badge')).toHaveText(['Online', 'Offline']);
  await page.getByLabel('Filter agents by status').selectOption('true');
  await expect(page.locator('tbody tr')).toHaveCount(1);
  await page.getByRole('button', { name: 'Inspect agent 40000000' }).click();
  await page.getByRole('button', { name: 'View claimed jobs' }).click();
  await expect(page.getByRole('heading', { name: 'Schedule', exact: true })).toBeVisible();
  await expect(page.getByText('Agent 40000000', { exact: true })).toBeVisible();
  expect(calls.some((call) => call.path === '/jobs')).toBe(true);
  await page.getByRole('button', { name: 'Clear job filter' }).click();
  await expect(page.getByRole('button', { name: 'Clear job filter' })).toHaveCount(0);
});

test('reports API errors and recovers on retry', async ({ page }) => {
  const { failures } = await backend(page);
  failures.set('/graph_drafts', 503);
  await page.goto('/');
  await expect(page.getByRole('alert')).toHaveText(/Database temporarily unavailable/);
  failures.delete('/graph_drafts');
  await page.getByRole('button', { name: 'Try again' }).click();
  await expect(page.getByRole('button', { name: 'Edit daily-data-pipeline' })).toBeVisible();
  await expect(page.getByRole('alert')).toHaveCount(0);
});

test('keeps navigation and content usable on mobile', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await backend(page);
  await page.goto('/');
  await page.getByRole('button', { name: 'Open navigation' }).click();
  await page.getByRole('link', { name: 'Agents Connected fleet' }).click();
  await expect(page.getByRole('heading', { name: 'Agents', exact: true })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test('renders the workflow studio without browser errors', async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  await backend(page);
  await page.goto('/');
  await page.getByRole('button', { name: 'Edit daily-data-pipeline' }).click();
  await expect(page.locator('.react-flow__node')).toHaveCount(4);
  await expect(page.getByText('Ready to publish', { exact: true })).toBeVisible();
  await page.screenshot({ path: 'test-results/workflow-studio.png', fullPage: true });
  expect(errors).toEqual([]);
});

test('persists node settings and validates resources, timeout, and Docker JSON', async ({ page }) => {
  const { calls } = await backend(page);
  await page.goto('/');
  await page.getByRole('button', { name: 'Edit daily-data-pipeline' }).click();
  await page.getByLabel('CPU cores', { exact: true }).fill('0');
  await expect(page.getByRole('button', { name: 'Publish', exact: true })).toBeDisabled();
  await page.getByLabel('CPU cores', { exact: true }).fill('4');
  await page.getByLabel('Memory (MiB)', { exact: true }).fill('512');
  await page.getByLabel('Timeout', { exact: true }).fill('0s');
  await expect(page.getByRole('button', { name: 'Publish', exact: true })).toBeDisabled();
  await page.getByLabel('Timeout', { exact: true }).fill('10s');
  await page.getByRole('button', { name: 'Edit Docker configuration JSON' }).click();
  await page.getByLabel('Docker configuration JSON', { exact: true }).fill('{');
  await page.getByRole('button', { name: 'Apply configuration' }).click();
  await expect(page.getByRole('alert')).toBeVisible();
  const config = { image: 'busybox', command: ['echo', 'hello'], env: ['MODE=test'], working_dir: '/tmp' };
  await page.getByLabel('Docker configuration JSON', { exact: true }).fill(JSON.stringify(config));
  await page.getByRole('button', { name: 'Apply configuration' }).click();
  await expect(page.getByLabel('Docker image', { exact: true })).toHaveValue('busybox');
  await expect(page.getByRole('button', { name: 'Publish', exact: true })).toBeEnabled();
  await page.getByRole('button', { name: 'Save draft' }).click();
  await expect(page.getByText('All changes saved')).toBeVisible();
  const saved = calls.find((call) => call.method === 'PUT')!.body as { graph: Graph };
  expect(saved.graph.nodes[0].settings).toEqual({ config, resource: { cpu_cores: 4, memory_bytes: 536870912 }, timeout: '10s' });
  expect(saved.graph.nodes[0]).not.toHaveProperty('config');
  await page.screenshot({ path: 'test-results/node-settings.png', fullPage: true });
});

test('selects outputs, adds dependencies, and preserves references through producer edits', async ({ page }) => {
  const { calls } = await backend(page, { empty: true });
  await page.goto('/');
  await page.getByRole('button', { name: 'New workflow' }).click();
  await page.getByLabel('Workflow name', { exact: true }).fill('output-bindings');
  await page.getByRole('button', { name: 'Create workflow', exact: true }).click();
  await page.getByLabel('Docker image', { exact: true }).fill('alpine:3.21');
  await page.getByRole('button', { name: 'Add output', exact: true }).click();
  await page.getByLabel('Output 1 file name').fill('result.json');
  await page.getByRole('button', { name: 'Add node', exact: true }).click();
  await page.getByLabel('Docker image', { exact: true }).fill('busybox');
  await page.getByRole('button', { name: 'Add input', exact: true }).click();
  await page.getByRole('combobox', { name: 'Input 1 source', exact: true }).selectOption('start.result.json');
  await expect(page.getByLabel('Input 1 file name')).toHaveValue('result.json');
  await expect(page.getByRole('button', { name: 'Publish', exact: true })).toBeEnabled();
  await page.getByRole('button', { name: 'Add output', exact: true }).click();
  await page.getByLabel('Output 1 file name').fill('processed.json');
  await page.locator('.react-flow__node').filter({ hasText: 'start' }).click();
  await page.getByLabel('Node name', { exact: true }).fill('source');
  await page.getByLabel('Output 1 file name').fill('renamed.json');
  await page.getByRole('button', { name: 'Add input', exact: true }).click();
  await expect(page.getByRole('combobox', { name: 'Input 1 source' }).locator('option')).toHaveCount(1);
  await page.getByRole('button', { name: 'Remove input 1', exact: true }).click();
  await page.locator('.react-flow__node').filter({ hasText: 'task_2' }).click();
  await expect(page.getByRole('combobox', { name: 'Input 1 source' })).toHaveValue('source.renamed.json');
  await expect(page.getByLabel('Input 1 file name')).toHaveValue('result.json');
  await page.getByRole('button', { name: 'Save draft' }).click();
  await expect(page.getByText('All changes saved')).toBeVisible();
  const saved = calls.find((call) => call.method === 'PUT')!.body as { graph: Graph };
  expect(saved.graph.edges).toEqual([[saved.graph.nodes[0].id, saved.graph.nodes[1].id]]);
  expect(saved.graph.nodes[1].inputs[0].source).toBe('source.renamed.json');
  await page.locator('.react-flow__node').filter({ hasText: 'source' }).click();
  await page.getByRole('button', { name: 'Remove output 1', exact: true }).click();
  await page.locator('.react-flow__node').filter({ hasText: 'task_2' }).click();
  await expect(page.getByRole('combobox', { name: 'Input 1 source' })).toHaveValue('');
  await expect(page.getByRole('button', { name: 'Publish', exact: true })).toBeDisabled();
});

test('clears broken bindings after removing a connection or producer on mobile', async ({ page }) => {
  await backend(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  await page.getByRole('button', { name: 'Edit daily-data-pipeline' }).click();
  await page.getByRole('button', { name: 'Edit JSON' }).click();
  const connected = structuredClone(graph);
  connected.nodes[0].outputs = [{ file_name: 'result.json', kind: 'File', multiple: false }];
  connected.nodes[1].inputs = [{ file_name: 'local.json', source: 'extract_data.result.json', required: true, format: null }];
  connected.nodes[3].inputs = structuredClone(connected.nodes[1].inputs);
  await page.getByRole('textbox', { name: 'Workflow JSON', exact: true }).fill(JSON.stringify(connected));
  await page.getByRole('button', { name: 'Apply changes' }).click();
  await page.getByRole('button', { name: 'Remove connection', exact: true }).first().click();
  await page.locator('.react-flow__node').filter({ hasText: 'clean_records' }).click();
  await expect(page.getByRole('combobox', { name: 'Input 1 source' })).toHaveValue('');
  await page.locator('.react-flow__node').filter({ hasText: 'publish_dataset' }).click();
  await expect(page.getByRole('combobox', { name: 'Input 1 source' })).toHaveValue('extract_data.result.json');
  await page.locator('.react-flow__node').filter({ hasText: 'extract_data' }).click();
  await page.getByRole('button', { name: 'Delete node', exact: true }).click();
  await page.locator('.react-flow__node').filter({ hasText: 'publish_dataset' }).click();
  await expect(page.getByRole('combobox', { name: 'Input 1 source' })).toHaveValue('');
  await expect(page.getByLabel('Input 1 file name')).toHaveValue('local.json');
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.screenshot({ path: 'test-results/node-settings-mobile.png', fullPage: true });
});
