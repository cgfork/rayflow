import { expect, test, type Locator, type Page } from '@playwright/test';
import { readFile } from 'node:fs/promises';
import type { Graph, GraphNode, Job, NodeRun, Run, RunView } from '../src/types';
import { expectDrawerAlignedWithInspector } from './drawer';

const timestamp = '2026-11-18T00:00:00Z';
const run: Run = {
  id: 1,
  uuid: 'fb000001-0000-4000-8000-000000000001',
  data_id: 'fb000002-0000-4000-8000-000000000002',
  graph_id: 'fb000003-0000-4000-8000-000000000003',
  version: 2,
  revision: 3,
  created_at: timestamp,
  updated_at: timestamp,
};
const detailPath = `/api/v1/runs/${run.uuid}`;

function graphNode(id: string, name: string): GraphNode {
  return {
    id, name, node_type: 'script',
    settings: { config: {}, resource: { cpu_cores: 1, memory_bytes: 1024 }, timeout: '10s' },
    inputs: [], outputs: [],
  };
}

const prepare = graphNode('fa000001-0000-4000-8000-000000000001', 'Prepare input');
const process = graphNode('fa000002-0000-4000-8000-000000000002', 'Process input');
const publish = graphNode('fa000003-0000-4000-8000-000000000003', 'Publish result');
const graph: Graph = {
  id: run.graph_id, name: 'Sample workflow', version: run.version, node_count: 3,
  nodes: [prepare, process, publish], edges: [[prepare.id, process.id], [process.id, publish.id]],
};

function nodeRun(id: number, uuid: string, node: GraphNode, revision: number, status: NodeRun['status']): NodeRun {
  return {
    ...run, id, uuid, revision, status, run_id: run.uuid, node_id: node.id, ineffective: 0,
  };
}

const succeeded = nodeRun(1, 'fc000001-0000-4000-8000-000000000001', prepare, 2, 'Succeeded');
const pending = nodeRun(2, 'fc000002-0000-4000-8000-000000000002', process, 3, 'Pending');
const prepareResult = { ...prepare, outputs: [{ file_name: 'prepared.json', kind: 'File' as const, multiple: false }] };
const view: RunView = { run, graph, node_runs: [[succeeded, prepareResult], [pending, process]] };

async function mockRuntime(page: Page) {
  await page.route('**/api/v1/auth/providers', (route) => route.fulfill({ json: { enabled: false, providers: [] } }));
  await page.route('**/api/v1/health', (route) => route.fulfill({ body: 'ok' }));
  await page.route('**/api/v1/graphs?limit=200', (route) => route.fulfill({ json: { items: [graph], next_cursor: null } }));
  await page.route(`**/api/v1/graphs/${graph.id}`, (route) => route.fulfill({ json: graph }));
  await page.route(`**/api/v1/runs?graph_id=${graph.id}&limit=50`, (route) => route.fulfill({ json: { items: [run], next_cursor: null } }));
  await page.route('**/api/v1/runs/*/node-runs/*/artifacts/*', (route) => route.fulfill({ json: null }));
}

async function selectWorkflow(page: Page) {
  await page.goto('/#/runtime');
  await page.getByRole('combobox', { name: 'Search workflows' }).fill(graph.name);
  const option = page.getByRole('option', { name: `${graph.name} ${graph.id}`, exact: true });
  await expect(option.getByTitle(graph.id, { exact: true })).toHaveText(graph.id.slice(0, 6));
  await option.click();
}

function inspectRun(page: Page) {
  return page.getByRole('button', { name: `Inspect run ${run.uuid}`, exact: true });
}

test('Node State loads result and usage on selection, manual refresh, and retry', async ({ page }) => {
  await mockRuntime(page);
  await page.route(`**${detailPath}`, (route) => route.fulfill({ json: view }));
  const storageOrigin = 'https://rayflow-archives.example.invalid';
  const makeArchive = (nodeRun: NodeRun, filename: string, body: object) => ({
    path: `${detailPath}/node-runs/${nodeRun.uuid}/artifacts/${filename}`,
    signed: {
      url: `${storageOrigin}/jobs/${nodeRun.uuid}/${filename}?X-Amz-Security-Token=fixture%2Btoken%3D&X-Amz-Signature=0123456789abcdef`,
      operation: 'download', headers: {}, expires_at: '2030-01-01T00:00:00Z',
    },
    body,
  });
  const result = makeArchive(succeeded, 'result.json', { outcome: 'succeed', exit_code: 0, duration_ms: 1250, finished_at: timestamp });
  const usage = makeArchive(succeeded, 'usage.json', {
    summary: { sampled_at: timestamp, peak_cpu_millicores: 750, peak_memory_usage_bytes: 536_870_912 },
  });
  const archives = [result, usage, makeArchive(pending, 'result.json', { outcome: 'yield', exit_code: 0 }), makeArchive(pending, 'usage.json', {})];
  let usageReady = false;
  let usageDownloadFails = true;
  let resultRequests = 0;
  let usageRequests = 0;
  for (const archive of archives) {
    await page.route(`**${archive.path}`, (route) => {
      if (archive === result) resultRequests += 1;
      if (archive === usage) usageRequests += 1;
      return route.fulfill({ json: archive === usage && !usageReady ? null : archive.signed });
    });
  }
  await page.route((url) => url.origin === storageOrigin, (route) => {
    const headers = { 'Access-Control-Allow-Origin': '*' };
    const archive = archives.find(({ signed }) => signed.url === route.request().url());
    if (!archive) return route.fulfill({ headers, status: 403 });
    if (archive === usage && usageDownloadFails) return route.fulfill({ headers, status: 503 });
    return route.fulfill({ headers, json: archive.body });
  });

  await page.clock.install();
  await selectWorkflow(page);
  await inspectRun(page).click();
  const canvas = page.getByTestId('flow-canvas');
  const graphPanel = page.locator('.runtime-canvas .canvas-panel');
  await expect(graphPanel).toBeVisible();
  const initialHeight = (await graphPanel.boundingBox())!.height;
  const [download] = await Promise.all([
    page.waitForRequest((request) => new URL(request.url()).origin === storageOrigin),
    canvas.getByText(prepare.name, { exact: true }).click(),
  ]);
  expect(download.url()).toBe(result.signed.url);
  expect(download.method()).toBe('GET');
  const headers = await download.allHeaders();
  expect(headers['x-rayflow-request']).toBeUndefined();
  expect(headers.referer).toBeUndefined();
  const resultPanel = page.getByRole('region', { name: 'Result' });
  const usagePanel = page.getByRole('region', { name: 'Usage' });
  await expect(resultPanel.getByText('Succeeded', { exact: true })).toBeVisible();
  await expect(resultPanel.getByText('1.25 s', { exact: true })).toBeVisible();
  await expect(usagePanel).toContainText('Usage has not been archived.');

  usageReady = true;
  await page.clock.fastForward(20_000);
  await expect(usagePanel).toContainText('Usage has not been archived.');
  expect([resultRequests, usageRequests]).toEqual([1, 1]);
  await Promise.all([
    page.waitForResponse((response) => response.url() === result.signed.url),
    page.getByRole('button', { name: 'Refresh', exact: true }).click(),
  ]);
  const usageError = usagePanel.getByRole('alert');
  await expect(usageError).toContainText('Archive download failed (HTTP 503).');
  await expect(resultPanel.getByText('Succeeded', { exact: true })).toBeVisible();
  expect([resultRequests, usageRequests]).toEqual([2, 2]);

  usageDownloadFails = false;
  await usageError.getByRole('button', { name: 'Try again' }).click();
  await expect(usagePanel).toContainText('0.75 cores');
  await expect(usagePanel).toContainText('512 MiB');
  expect([resultRequests, usageRequests]).toEqual([2, 3]);
  expect((await graphPanel.boundingBox())!.height).toBeCloseTo(initialHeight, 1);
  const inspector = page.getByRole('complementary').filter({ hasText: 'NODE STATE' });
  const scrollTop = await inspector.evaluate((element) => {
    element.scrollTop = element.scrollHeight;
    return element.scrollTop;
  });
  expect(scrollTop).toBeGreaterThan(0);

  await canvas.getByText(process.name, { exact: true }).click();
  await expect(resultPanel.getByText('Yielded', { exact: true })).toBeVisible();
  await expect(usagePanel).toContainText('No usage samples were recorded.');
  await expect(usagePanel).not.toContainText('512 MiB');
});

test('Node State matches the Execution Graph height on desktop and the panel meets the footer', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1400 });
  await mockRuntime(page);
  await page.route(`**${detailPath}`, (route) => route.fulfill({ json: view }));
  await selectWorkflow(page);
  await inspectRun(page).click();

  const panel = page.locator('.runtime-canvas').filter({ hasText: 'Execution graph' });
  const footer = page.getByRole('contentinfo').filter({ hasText: 'Workflow console' });
  await expect(panel).toBeVisible();
  await expect(footer).toBeVisible();
  const graph = (await panel.locator('.canvas-panel').boundingBox())!;
  const inspector = (await panel.getByRole('complementary').boundingBox())!;
  expect(inspector.x).toBeCloseTo(graph.x + graph.width, 1);
  expect(inspector.y).toBeCloseTo(graph.y, 1);
  expect(inspector.height).toBeCloseTo(graph.height, 1);
  const desktopPanel = (await panel.boundingBox())!;
  const desktopFooter = (await footer.boundingBox())!;
  expect(desktopPanel.y + desktopPanel.height).toBeCloseTo(desktopFooter.y, 1);
  expect(desktopFooter.y + desktopFooter.height).toBeCloseTo(1400, 1);

  await page.setViewportSize({ width: 320, height: 844 });
  const mobileGraph = (await panel.locator('.canvas-panel').boundingBox())!;
  const mobileInspector = (await panel.getByRole('complementary').boundingBox())!;
  expect(mobileInspector.y).toBeCloseTo(mobileGraph.y + mobileGraph.height, 1);
  expect(mobileInspector.width).toBeCloseTo(mobileGraph.width, 1);
  const mobilePanel = (await panel.boundingBox())!;
  const mobileFooter = (await footer.boundingBox())!;
  expect(mobilePanel.y + mobilePanel.height).toBeCloseTo(mobileFooter.y, 1);
});

declare global {
  interface Window { runDetailSignals: (AbortSignal | null)[] }
}

test('Inspect loads the execution graph once and updates it on manual refresh', async ({ page }) => {
  const running: RunView = { ...view, node_runs: [[succeeded, prepareResult], [{ ...pending, status: 'Running' }, process]] };
  let response: RunView = view;
  let fail = false;
  await mockRuntime(page);
  await page.route(`**${detailPath}`, (route) => fail
    ? route.fulfill({ status: 500, json: { error: 'Unable to load run' } })
    : route.fulfill({ json: response }));
  await page.addInitScript((path) => {
    const originalFetch = window.fetch.bind(window);
    window.runDetailSignals = [];
    window.fetch = (input, init) => {
      if (input === path) window.runDetailSignals.push(init?.signal ?? null);
      return originalFetch(input, init);
    };
  }, detailPath);
  const signals = () => page.evaluate(() => window.runDetailSignals.map((signal) => signal?.aborted));
  await page.clock.install();
  await selectWorkflow(page);
  await expect(page.getByTitle(run.uuid, { exact: true })).toHaveText(`Run ${run.uuid.slice(0, 6)}`);
  await expect(page.getByTitle(run.data_id, { exact: true })).toHaveText(`Data ${run.data_id.slice(0, 6)}`);
  await inspectRun(page).click();

  await expect(page.getByRole('heading', { name: graph.name })).toBeVisible();
  await expect(page.getByTitle(run.uuid, { exact: true })).toHaveText(`RUN ${run.uuid.slice(0, 6)}`);
  await expect(page.getByTitle(run.data_id, { exact: true })).toHaveText(run.data_id.slice(0, 6));
  expect(await signals()).toEqual([false]);
  await expect(page.getByText('Revision 3', { exact: true })).toBeVisible();
  const canvas = page.getByTestId('flow-canvas');
  await expect(canvas.getByText('Succeeded', { exact: true })).toBeVisible();
  await expect(canvas.getByText('Pending', { exact: true })).toBeVisible();
  await expect(canvas.getByText('Waiting', { exact: true })).toBeVisible();

  response = running;
  await page.clock.fastForward(20_000);
  await expect(canvas.getByText('Pending', { exact: true })).toBeVisible();
  expect(await signals()).toEqual([false]);
  const refresh = page.getByRole('button', { name: 'Refresh', exact: true });
  await refresh.click();
  await expect(canvas.getByText('Running', { exact: true })).toBeVisible();
  expect(await signals()).toEqual([true, false]);

  fail = true;
  await refresh.click();
  const error = page.getByRole('alert');
  await expect(error.getByText('Unable to load run')).toBeVisible();
  expect(await signals()).toHaveLength(3);

  fail = false;
  await error.getByRole('button', { name: 'Try again' }).click();
  await expect(error).toHaveCount(0);
  expect(await signals()).toHaveLength(4);

  await page.getByRole('button', { name: 'All runs' }).click();
  await page.clock.fastForward(20_000);
  expect(await signals()).toEqual([true, true, true, true]);
  await inspectRun(page).click();
  await expect(page.getByRole('heading', { name: graph.name })).toBeVisible();
  expect(await signals()).toHaveLength(5);
});

test('Runtime shows node run ports and offers eligible node commands', async ({ page }) => {
  await mockRuntime(page);
  await page.route(`**${detailPath}`, (route) => route.fulfill({ json: view }));
  await page.route('**/api/v1/runs/{rerun,resume,upgrade}', (route) => route.fulfill({ status: 202, json: { id: 1 } }));
  await page.route(`**/api/v1/graphs?graph_id=${graph.id}&limit=50`, (route) => route.fulfill({ json: { items: [{ ...graph, version: 3 }, graph], next_cursor: null } }));
  await page.route(`**/api/v1/graphs/${graph.id}/shared-nodes?from_version=2&version=3`, (route) => route.fulfill({ json: [prepare.id, process.id] }));
  const submit = async (path: string, button: Locator) => {
    const [request] = await Promise.all([page.waitForRequest(`**/api/v1${path}`), button.click()]);
    return request.postDataJSON();
  };
  const command = { graph_id: run.graph_id, data_id: run.data_id };
  const canvas = page.getByTestId('flow-canvas');
  const inspector = page.getByRole('complementary').filter({ hasText: 'NODE STATE' });
  await selectWorkflow(page);
  await inspectRun(page).click();

  await canvas.getByText(prepare.name, { exact: true }).click();
  await expect(inspector.getByText('#2', { exact: true })).toBeVisible();
  const resumeNode = inspector.getByRole('button', { name: 'Resume node' });
  const rerunNode = inspector.getByRole('button', { name: 'Rerun node' });
  await expect(resumeNode).toBeDisabled();
  const firstDetail = (await inspector.getByText('Type', { exact: true }).boundingBox())!;
  for (const action of [resumeNode, rerunNode]) {
    const bounds = (await action.boundingBox())!;
    expect(bounds.y + bounds.height).toBeLessThan(firstDetail.y);
  }
  const outputFiles = inspector.getByRole('button', { name: 'View output files', exact: true });
  await outputFiles.click();
  const outputDrawer = page.getByRole('dialog', { name: 'Output files', exact: true });
  await expect(outputDrawer.getByRole('button', { name: 'Download output prepared.json' })).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(outputDrawer).toBeHidden();
  await expect(outputFiles).toBeFocused();

  await rerunNode.click();
  const rerun = page.getByRole('dialog', { name: 'Rerun workflow' });
  await expect(rerun.getByLabel('Start from node')).toHaveValue(prepare.id);
  expect(await submit('/runs/rerun', rerun.getByRole('button', { name: 'Confirm command' }))).toEqual({ ...command, node: prepare.id });
  await expect(rerun).toBeHidden();

  await canvas.getByText(process.name, { exact: true }).click();
  await expect(rerunNode).toBeDisabled();
  await resumeNode.click();
  const resume = page.getByRole('dialog', { name: `Resume ${process.name}?` });
  expect(await submit('/runs/resume', resume.getByRole('button', { name: 'Confirm command' }))).toEqual({ ...command, node: process.id });
  await expect(resume).toBeHidden();

  await page.getByRole('button', { name: 'Upgrade', exact: true }).click();
  const upgrade = page.getByRole('dialog', { name: 'Upgrade workflow' });
  await upgrade.getByLabel('Target version').selectOption('3');
  await upgrade.getByLabel('Shared restart node').selectOption(prepare.id);
  expect(await submit('/runs/upgrade', upgrade.getByRole('button', { name: 'Confirm upgrade' }))).toEqual({ ...command, node: prepare.id, version: 3 });
  await expect(upgrade).toBeHidden();

  const width = 320;
  await page.setViewportSize({ width, height: 844 });
  for (const value of [run.uuid, run.data_id]) {
    const id = page.getByTitle(value, { exact: true });
    await expect(id).toBeVisible();
    const fits = await id.evaluate((element) => {
      const range = document.createRange();
      range.selectNodeContents(element);
      return Array.from(range.getClientRects()).every((line) => line.left >= 0 && line.right <= window.innerWidth);
    });
    expect(fits).toBe(true);
  }
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);

  const job: Job = {
    id: 1, uuid: 'fd000001-0000-4000-8000-000000000001', task_id: 'fd000002-0000-4000-8000-000000000002',
    run_id: run.uuid, node_run_id: 1, status: 'Scheduled', agent_id: null,
    required_cpu_cores: 1, required_memory_bytes: 1048576, polled_at: null, expired_at: null,
    created_at: timestamp, updated_at: timestamp,
  };
  await page.route(`**/api/v1/jobs?run_id=${run.uuid}&limit=50`, (route) => route.fulfill({ json: { items: [job], next_cursor: null } }));
  await page.getByRole('button', { name: 'View jobs', exact: true }).click();
  await expect(page).toHaveURL(/#\/schedule$/);
  const runLabels = page.getByTitle(run.uuid, { exact: true });
  const caption = `Run ${run.uuid.slice(0, 6)}`;
  await expect(runLabels).toHaveText([caption, caption]);
  const row = page.getByRole('row').filter({ has: runLabels });
  expect(await row.evaluate((element) => element.getBoundingClientRect().height)).toBeLessThan(220);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
});

test('Node file drawers align to the inspector and preserve download eligibility and revision', async ({ page }) => {
  await page.emulateMedia({ colorScheme: 'dark' });
  await mockRuntime(page);
  const fileName = 'warehouse counts.csv';
  const otherFile = 'warehouse manifest.json';
  const pendingOutputs = Array.from({ length: 24 }, (_, index) => ({ file_name: `report-${index + 1}.json`, kind: 'File' as const, multiple: false }));
  const inputNode = { ...process, inputs: [
    { file_name: fileName, source: 'prepare.counts.csv', required: true, format: 'csv' },
    { file_name: otherFile, source: 'prepare.manifest.json', required: false, format: 'json' },
  ], outputs: pendingOutputs };
  await page.route(`**${detailPath}`, (route) => route.fulfill({ json: { ...view, node_runs: [[succeeded, prepareResult], [pending, inputNode]] } }));
  const downloadPath = `**${detailPath}/files/download`;
  const storageOrigin = 'https://rayflow-artifacts.example.invalid';
  const objects = [
    { name: fileName, key: 'objects/source counts.csv', body: 'warehouse,count\nShenzhen,12\n' },
    { name: 'prepared.json', key: 'objects/prepared.json', body: '{"available":true}\n' },
  ].map((object) => ({
    ...object,
    url: `${storageOrigin}/${object.key.split('/').map(encodeURIComponent).join('/')}`
      + '?X-Amz-Credential=fixture%2F20261118%2Fus-east-1%2Fs3%2Faws4_request'
      + '&X-Amz-Security-Token=example%2Bsession%2Ftoken%3D&X-Amz-SignedHeaders=host'
      + `&response-content-disposition=${encodeURIComponent(`attachment; filename="${object.name}"`)}`
      + '&X-Amz-Signature=0123456789abcdef',
  }));
  const requests: string[] = [];
  const legacyRequests: string[] = [];
  const context = page.context();
  context.on('request', (request) => {
    if (new URL(request.url()).pathname.endsWith('/files/content')) legacyRequests.push(request.url());
  });
  let reply: 'unavailable' | 'invalid' | 'signed' = 'unavailable';
  await page.route(downloadPath, (route) => {
    requests.push('presign');
    if (reply === 'unavailable') return route.fulfill({ status: 503, json: { error: 'File temporarily unavailable' } });
    const object = objects.find(({ name }) => name === route.request().postDataJSON().file_name)!;
    return route.fulfill({ json: {
      catalog: {
        id: 1, uuid: 'fd000003-0000-4000-8000-000000000003', key: object.key, url: `s3://rayflow-fixtures/${object.key}`,
        size: Buffer.byteLength(object.body), format: object.name.split('.').at(-1), uploaded: true,
        created_at: timestamp, updated_at: timestamp,
      },
      presigned: { url: reply === 'invalid' ? '/missing-signature' : object.url, operation: 'download', headers: {}, expires_at: '2030-01-01T00:00:00Z' },
      multipart: true,
    } });
  });
  // A navigation to the signed URL can be the first request in a new tab.
  await context.route((url) => url.origin === storageOrigin, (route) => {
    requests.push('storage');
    const object = objects.find(({ url }) => url === route.request().url());
    if (!object) return route.fulfill({ status: 403, body: 'Signed URL mismatch' });
    return route.fulfill({
      headers: { 'Content-Type': 'application/octet-stream', 'Content-Disposition': `attachment; filename="${object.name}"` },
      body: object.body,
    });
  });
  await selectWorkflow(page);
  await inspectRun(page).click();
  const canvas = page.getByTestId('flow-canvas');
  await canvas.getByText(process.name, { exact: true }).click();
  const inspector = page.getByRole('complementary').filter({ hasText: 'NODE STATE' });
  await expect(inspector.getByRole('heading', { name: /^(Input|Output) files$/ })).toHaveCount(0);
  const trigger = inspector.getByRole('button', { name: 'View input files', exact: true });
  await trigger.focus();
  await trigger.press('Enter');
  const drawer = page.getByRole('dialog', { name: 'Input files', exact: true });
  await expectDrawerAlignedWithInspector(drawer);
  const download = drawer.getByRole('button', { name: `Download input ${fileName}`, exact: true });
  await expect(drawer.getByRole('button', { name: /^Download input / })).toHaveCount(2);
  await expect(drawer.getByRole('button', { name: `Download input ${otherFile}`, exact: true })).toBeVisible();
  await expect(drawer.getByText('prepared.json', { exact: true })).toHaveCount(0);
  expect(requests).toEqual([]);
  await download.click();
  await expect(drawer.getByRole('alert')).toContainText('File temporarily unavailable');
  expect(requests).toEqual(['presign']);

  reply = 'invalid';
  await download.click();
  await expect(drawer.getByRole('alert')).toContainText('The server did not return a valid download link.');
  expect(requests).toEqual(['presign', 'presign']);

  const expectFileDownload = async (button: Locator, direction: 'input' | 'output', nodeId: string, name: string) => {
    const expectedObject = objects.find((object) => object.name === name)!;
    const currentPage = page.url();
    const [presign, fileRequest, file] = await Promise.all([
      page.waitForRequest(downloadPath),
      context.waitForEvent('request', { predicate: (request) => new URL(request.url()).origin === storageOrigin }),
      page.waitForEvent('download'),
      button.click(),
    ]);
    const command = { node_id: nodeId, direction, file_name: name, revision: run.revision };
    expect(presign.method()).toBe('POST');
    expect(presign.postDataJSON()).toEqual(command);
    expect((await presign.allHeaders())['x-rayflow-request']).toBe('1');
    expect(fileRequest.method()).toBe('GET');
    expect(fileRequest.url()).toBe(expectedObject.url);
    const storageHeaders = await fileRequest.allHeaders();
    expect(storageHeaders['x-rayflow-request']).toBeUndefined();
    expect(storageHeaders.authorization).toBeUndefined();
    expect(storageHeaders.referer).toBeUndefined();
    expect(file.suggestedFilename()).toBe(name);
    expect(await readFile(await file.path(), 'utf8')).toBe(expectedObject.body);
    await expect(page).toHaveURL(currentPage);
  };
  reply = 'signed';
  await expectFileDownload(download, 'input', process.id, fileName);
  await expect(drawer.getByRole('alert')).toHaveCount(0);
  expect(requests).toEqual(['presign', 'presign', 'presign', 'storage']);

  const desktop = page.viewportSize()!;
  await page.setViewportSize({ width: 320, height: 844 });
  await expectDrawerAlignedWithInspector(drawer);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(320);
  await page.setViewportSize(desktop);
  await expectDrawerAlignedWithInspector(drawer);
  await page.keyboard.press('Escape');
  await expect(drawer).toBeHidden();
  await expect(trigger).toBeFocused();
  await trigger.press('Enter');
  await expect(drawer).toBeVisible();
  await page.mouse.click(10, 10);
  await expect(drawer).toBeHidden();
  await expect(trigger).toBeFocused();

  const outputTrigger = inspector.getByRole('button', { name: 'View output files', exact: true });
  const outputDrawer = page.getByRole('dialog', { name: 'Output files', exact: true });
  await outputTrigger.click();
  await expectDrawerAlignedWithInspector(outputDrawer);
  await expect(outputDrawer.getByText(pendingOutputs[0].file_name, { exact: true })).toBeVisible();
  await expect(outputDrawer.getByRole('button', { name: /^Download output / })).toHaveCount(0);
  const scrolled = await outputDrawer.locator('.drawer-body').evaluate((body) => {
    body.scrollTop = body.scrollHeight;
    return body.scrollTop > 0;
  });
  expect(scrolled).toBe(true);
  await expectDrawerAlignedWithInspector(outputDrawer);
  await page.keyboard.press('Escape');
  await expect(outputTrigger).toBeFocused();

  await canvas.getByText(prepare.name, { exact: true }).click();
  await outputTrigger.press('Enter');
  await expectDrawerAlignedWithInspector(outputDrawer);
  await expectFileDownload(outputDrawer.getByRole('button', { name: 'Download output prepared.json', exact: true }), 'output', prepare.id, 'prepared.json');
  await outputDrawer.getByRole('button', { name: 'Close dialog', exact: true }).click();
  await expect(outputTrigger).toBeFocused();
  await trigger.click();
  await expect(drawer.getByText('No input files.', { exact: true })).toBeVisible();
  await page.setViewportSize({ width: 320, height: 844 });
  await expectDrawerAlignedWithInspector(drawer);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(320);
  await drawer.getByRole('button', { name: 'Close dialog', exact: true }).click();
  await expect(trigger).toBeFocused();
  expect(legacyRequests).toEqual([]);
});

test('Runtime UUIDs reveal and copy the full identifier without opening the run', async ({ page, context }) => {
  await context.grantPermissions(['clipboard-read', 'clipboard-write']);
  await page.emulateMedia({ colorScheme: 'light' });
  await mockRuntime(page);
  await page.route(`**${detailPath}`, (route) => route.fulfill({ json: view }));
  await selectWorkflow(page);
  const table = page.getByRole('table');
  for (const [kind, id] of [['run', run.uuid], ['data', run.data_id]]) {
    const copy = table.getByRole('button', { name: `Copy ${kind} UUID`, exact: true });
    const tooltip = page.getByRole('tooltip').filter({ hasText: id });
    expect(await copy.evaluate((element) => element.parentElement?.closest('button'))).toBeNull();
    await table.getByTitle(id, { exact: true }).hover();
    await expect(tooltip).toHaveText(id);
    await tooltip.hover();
    await expect(tooltip).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(tooltip).toBeHidden();
    await copy.focus();
    await expect(tooltip).toBeVisible();
    await copy.click();
    await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe(id);
    await expect(inspectRun(page)).toBeVisible();
    await page.getByRole('button', { name: 'Dismiss message' }).click();
  }
  await inspectRun(page).click();
  await expect(page.getByRole('heading', { name: graph.name, exact: true })).toBeVisible();
  await page.setViewportSize({ width: 320, height: 844 });
  const dataCopy = page.getByRole('button', { name: 'Copy data UUID', exact: true });
  const runCopy = page.getByRole('button', { name: 'Copy run UUID', exact: true });
  for (const colorScheme of ['light', 'dark'] as const) {
    await page.emulateMedia({ colorScheme });
    await expect(page.locator('html')).toHaveAttribute('data-theme', colorScheme);
    for (const [copy, id] of [[runCopy, run.uuid], [dataCopy, run.data_id]] as const) {
      await copy.focus();
      const tooltip = page.getByRole('tooltip').filter({ hasText: id });
      await expect(tooltip).toHaveText(id);
      const bounds = await tooltip.boundingBox();
      expect(bounds).not.toBeNull();
      expect(bounds!.x).toBeGreaterThanOrEqual(0);
      expect(bounds!.x + bounds!.width).toBeLessThanOrEqual(320);
    }
  }
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(320);

  await page.evaluate(() => {
    Object.defineProperty(navigator, 'clipboard', { configurable: true, value: undefined });
    document.execCommand = (command) => {
      const field = document.querySelector('textarea[readonly]');
      if (command !== 'copy' || !(field instanceof HTMLTextAreaElement)) return false;
      sessionStorage.setItem('fallbackSelection', field.value.slice(field.selectionStart, field.selectionEnd));
      return true;
    };
  });
  await runCopy.click();
  await expect(page.getByText('Run UUID copied.', { exact: true })).toBeVisible();
  expect(await page.evaluate(() => sessionStorage.getItem('fallbackSelection'))).toBe(run.uuid);
  await page.getByRole('button', { name: 'Dismiss message' }).click();
  await page.evaluate(() => {
    Object.defineProperty(navigator, 'clipboard', { configurable: true, value: {
      async writeText() { throw new DOMException('Clipboard denied', 'NotAllowedError'); },
    } });
    document.execCommand = () => false;
  });
  await dataCopy.click();
  const manual = page.getByRole('dialog', { name: 'Copy data UUID', exact: true });
  const input = manual.getByRole('textbox', { name: 'Data UUID', exact: true });
  await expect(input).toHaveValue(run.data_id);
  await expect(input).toBeFocused();
  await expect.poll(() => input.evaluate((field: HTMLInputElement) => [field.selectionStart, field.selectionEnd])).toEqual([0, run.data_id.length]);
  await expect(page.getByText('Data UUID copied.', { exact: true })).toHaveCount(0);
});
