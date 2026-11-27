import { createHash } from 'node:crypto';
import { expect, test, type Locator, type Page } from '@playwright/test';
import type { AssetTransfer, AssetVersion, Catalog, Graph, GraphNode, PrepareAssetResponse, PublishedAsset, Run } from '../src/types';

const api = '/api/v1';
const home = '/#/orchestration';
const cursor = 42;
const publishedCursor = 81;
const auditLabels = ['Created by', 'Created at', 'Updated by', 'Updated at'];

function graph(id: string, name: string, version: number, nodeNames: string[]): Graph {
  const nodes = nodeNames.map((name, index): GraphNode => ({
    id: `${id.slice(0, 24)}${String(index + 1).padStart(12, '0')}`,
    name,
    node_type: 'docker',
    settings: { config: { image: 'example.invalid/worker' }, resource: { cpu_cores: 1, memory_bytes: 1048576 }, timeout: '30s' },
    inputs: [],
    outputs: [],
    assets: [],
  }));
  return { id, name, version, node_count: nodes.length, nodes, edges: nodes.slice(1).map((node, index) => [nodes[index].id, node.id]) };
}

const ingest = {
  ...graph('ab000001-0000-4000-8000-000000000000', 'Zebra data ingest', 2, ['Collect', 'Normalize', 'Archive']),
  created_by: 'maya.chen',
  created_at: '2026-11-15T02:30:00Z',
  updated_by: 'leo.park',
  updated_at: '2026-11-17T08:45:00Z',
};
const report = graph('ab000002-0000-4000-8000-000000000000', 'Alpha reporting', 0, ['Summarize']);
const longName = 'ContinuousTelemetryAggregationWithAnIntentionallyUnbrokenWorkflowTitleForTheResponsiveTable';
const telemetry = graph('ab000003-0000-4000-8000-000000000000', longName, 0, ['Aggregate']);
const history = graph('ab000004-0000-4000-8000-000000000000', 'Beta history import', 0, ['Import']);
const ingestVersion = {
  ...ingest,
  name: 'Published data ingest',
  created_by: 'release.operator',
  created_at: '2026-11-18T03:15:00Z',
  updated_by: 'deployment.service',
  updated_at: '2026-11-18T03:20:00Z',
};
const cleanup = graph('ab000005-0000-4000-8000-000000000000', 'Scheduled cleanup', 4, ['Remove expired records']);
const olderCleanup = graph(cleanup.id, cleanup.name, 2, ['List candidates', 'Remove expired records']);
const firstCleanup = { ...cleanup, version: 1 };

async function mockGraphs(page: Page) {
  await page.route('**/api/v1/**', (route) => route.fulfill({ status: 501, json: { error: 'Unexpected API request' } }));
  await page.route(`**${api}/auth/providers`, (route) => route.fulfill({ json: { enabled: false, providers: [] } }));
  await page.route(`**${api}/health`, (route) => route.fulfill({ body: 'ok' }));
  await page.route(`**${api}/graph_drafts?limit=50`, (route) => route.fulfill({ json: { items: [ingest, report, telemetry], next_cursor: cursor } }));
  await page.route(`**${api}/graph_drafts?limit=50&before=${cursor}`, (route) => route.fulfill({ json: { items: [history], next_cursor: null } }));
  await page.route(`**${api}/graphs?limit=50`, (route) => route.fulfill({ json: { items: [cleanup, ingestVersion, olderCleanup], next_cursor: publishedCursor } }));
  await page.route(`**${api}/graphs?limit=50&before=${publishedCursor}`, (route) => route.fulfill({ json: { items: [firstCleanup], next_cursor: null } }));
}

function draftTable(page: Page) {
  return page.getByRole('table', { name: 'Draft workflows', exact: true });
}

function draftRow(page: Page, name: string) {
  return draftTable(page).getByRole('row').filter({ has: page.getByRole('button', { name, exact: true }) });
}

async function expectAuditIcons(container: Locator) {
  for (const label of auditLabels) {
    const iconLabel = container.getByTitle(label, { exact: true });
    await expect(iconLabel.locator('svg[aria-hidden=true]')).toBeVisible();
    const text = iconLabel.locator('.sr-only');
    await expect(text).toHaveText(label);
    await expect(text).toHaveCSS('clip-path', 'inset(50%)');
  }
}

async function expectAuditTimesOnOneLine(container: Locator) {
  const times = container.locator('time');
  await expect(times).toHaveCount(2);
  const lineCounts = await times.evaluateAll((elements) => elements.map((element) => {
    const range = document.createRange();
    range.selectNodeContents(element);
    return range.getClientRects().length;
  }));
  expect(lineCounts).toEqual([1, 1]);
}

async function expectModalInputUncovered(input: Locator) {
  await expect(input).toBeVisible();
  const geometry = await input.evaluate((element) => {
    const dialog = element.closest('dialog')!;
    const dialogBounds = dialog.getBoundingClientRect();
    const footerBounds = dialog.querySelector('.modal-actions')!.getBoundingClientRect();
    const bounds = element.getBoundingClientRect();
    return {
      withinDialog: bounds.top >= dialogBounds.top && bounds.bottom <= dialogBounds.bottom,
      aboveFooter: bounds.bottom <= footerBounds.top,
      lowerEdgeReachable: document.elementFromPoint(bounds.x + bounds.width / 2, bounds.bottom - 2) === element,
    };
  });
  expect(geometry).toEqual({ withinDialog: true, aboveFooter: true, lowerEdgeReachable: true });
}

function publishedTable(page: Page) {
  return page.getByRole('table', { name: 'Published versions', exact: true });
}

function publishedRow(page: Page, graph: Graph) {
  return publishedTable(page).getByRole('row')
    .filter({ has: page.getByRole('button', { name: graph.name, exact: true }) })
    .filter({ has: page.getByRole('cell', { name: `v${graph.version}`, exact: true }) });
}

test('Ready to publish has equal spacing above and below on desktop and mobile', async ({ page }) => {
  const margin = 8;
  const desktop = { width: 1440, height: 1400 };
  await page.setViewportSize(desktop);
  await mockGraphs(page);
  await page.route(`**${api}/graphs/${report.id}/draft`, (route) => route.fulfill({ json: report }));
  await page.goto(home);
  await draftRow(page, report.name).getByRole('button', { name: 'Edit', exact: true }).click();

  const studio = page.locator('.studio');
  const panels = studio.locator('.studio-body');
  const canvas = studio.locator('.canvas-panel');
  const inspector = studio.getByRole('complementary');
  const validation = studio.locator('.validation-panel').filter({ hasText: 'Ready to publish' });
  const footer = page.getByRole('contentinfo').filter({ hasText: 'Workflow console' });
  await expect(validation).toBeVisible();

  const desktopCanvas = (await canvas.boundingBox())!;
  const desktopInspector = (await inspector.boundingBox())!;
  const desktopPanels = (await panels.boundingBox())!;
  const desktopValidation = (await validation.boundingBox())!;
  const desktopFooter = (await footer.boundingBox())!;
  expect(desktopInspector.y).toBeCloseTo(desktopCanvas.y, 1);
  expect(desktopInspector.height).toBeCloseTo(desktopCanvas.height, 1);
  expect(desktopValidation.y - (desktopPanels.y + desktopPanels.height)).toBeCloseTo(margin, 1);
  expect(desktopFooter.y - (desktopValidation.y + desktopValidation.height)).toBeCloseTo(margin, 1);
  expect(desktopFooter.y + desktopFooter.height).toBeCloseTo(desktop.height, 1);

  await page.setViewportSize({ width: 320, height: 844 });
  const mobileCanvas = (await canvas.boundingBox())!;
  const mobileInspector = (await inspector.boundingBox())!;
  const mobilePanels = (await panels.boundingBox())!;
  const mobileValidation = (await validation.boundingBox())!;
  const mobileFooter = (await footer.boundingBox())!;
  expect(mobileInspector.width).toBeCloseTo(mobileCanvas.width, 1);
  expect(mobileInspector.y).toBeCloseTo(mobileCanvas.y + mobileCanvas.height, 1);
  expect(mobileValidation.y - (mobilePanels.y + mobilePanels.height)).toBeCloseTo(margin, 1);
  expect(mobileFooter.y - (mobileValidation.y + mobileValidation.height)).toBeCloseTo(margin, 1);
});

test('CEL annotation overrides can be added, edited, saved, and removed before publish', async ({ page }) => {
  await mockGraphs(page);
  const node = report.nodes[0];
  const existing = { annotation_match: 'has(annotations.dataset)', config: null, resource: null, timeout: '2m' };
  let draft: Graph = { ...report, nodes: [{ ...node, settings: { ...node.settings, overrides: { legacy: existing } } }] };
  let published: Graph | undefined;
  await page.route(`**${api}/graphs/${report.id}/draft`, (route) => {
    if (route.request().method() === 'PUT') draft = route.request().postDataJSON().graph;
    return route.fulfill({ json: draft });
  });
  await page.route(`**${api}/graphs/${report.id}/publish`, (route) => {
    published = { ...draft, version: 1 };
    return route.fulfill({ status: 201, json: published });
  });

  await page.goto(home);
  await draftRow(page, report.name).getByRole('button', { name: 'Edit', exact: true }).click();
  const gpuMemory = page.getByRole('spinbutton', { name: 'GPU memory (MiB)', exact: true });
  const gpuDevice = page.getByRole('textbox', { name: 'GPU CDI device', exact: true });
  await expect(gpuMemory).toHaveValue('');
  await gpuDevice.fill('nvidia.com/gpu=0');
  await expect(page.getByRole('button', { name: 'Publish', exact: true })).toBeDisabled();
  await gpuMemory.fill('0');
  await expect(page.getByRole('button', { name: 'Publish', exact: true })).toBeDisabled();
  await gpuMemory.fill('2048');
  await page.getByRole('button', { name: 'Edit override legacy' }).click();
  const edit = page.getByRole('dialog', { name: 'Edit annotation override' });
  await expect(edit.getByLabel('Annotation match (CEL)')).toHaveValue(existing.annotation_match);
  await expect(edit.getByRole('textbox', { name: 'Docker configuration JSON' })).toHaveText('');
  await expect(edit.getByLabel('Override resources')).not.toBeChecked();
  await expect(edit.getByLabel('Timeout')).toHaveValue(existing.timeout);
  await edit.getByLabel('Override name').fill('dataset');
  await edit.getByLabel('Timeout').clear();
  await edit.getByRole('button', { name: 'Apply override' }).click();
  await expect(page.getByRole('button', { name: 'Edit override legacy' })).toHaveCount(0);

  await page.getByRole('button', { name: 'Add override' }).click();
  const add = page.getByRole('dialog', { name: 'Add annotation override' });
  const apply = add.getByRole('button', { name: 'Apply override' });
  await add.getByLabel('Override name').fill('dataset');
  await apply.click();
  await expect(add.getByRole('alert')).toContainText('An override with this name already exists.');

  const annotationMatch = 'has(annotations.priority) &&\nannotations.priority == "high"';
  const config = { image: 'example.invalid/priority-worker', env: ['MODE=fast'] };
  await add.getByLabel('Override name').fill('priority');
  await add.getByLabel('Annotation match (CEL)').fill(annotationMatch);
  await add.getByLabel('Timeout').fill('45s');
  await add.getByLabel('Override resources').check();
  await add.getByLabel('CPU cores').fill('2');
  await add.getByLabel('Memory (MiB)', { exact: true }).fill('64');
  await add.getByLabel('GPU memory (MiB)').fill('4096');
  await add.getByLabel('GPU CDI device').fill('nvidia.com/gpu=1');
  await add.getByRole('textbox', { name: 'Docker configuration JSON' }).fill(JSON.stringify(config, null, 2));
  await apply.click();
  await expect(add).toBeHidden();

  await page.getByRole('button', { name: 'Save draft', exact: true }).click();
  await expect(page.getByText('Draft saved.', { exact: true })).toBeVisible();
  const priority = { annotation_match: annotationMatch, config, resource: { cpu_cores: 2, memory_bytes: 64 * 1048576, gpu_memory_bytes: 4096 * 1048576, gpu_cdi_device: 'nvidia.com/gpu=1' }, timeout: '45s' };
  expect(draft.nodes[0].settings).toEqual({
    ...node.settings,
    resource: { ...node.settings.resource, gpu_memory_bytes: 2048 * 1048576, gpu_cdi_device: 'nvidia.com/gpu=0' },
    overrides: { dataset: { annotation_match: existing.annotation_match }, priority },
  });

  await gpuDevice.clear();
  await gpuMemory.clear();
  await page.getByRole('button', { name: 'Remove override dataset' }).click();
  await page.getByRole('button', { name: 'Publish', exact: true }).click();
  await page.getByRole('dialog', { name: 'Publish workflow' }).getByRole('button', { name: 'Publish version' }).click();
  await expect(page.getByText(`Published ${report.name} · v1.`, { exact: true })).toBeVisible();
  expect(published?.nodes[0].settings.resource).toEqual(node.settings.resource);
  expect(published?.nodes[0].settings.overrides).toEqual({ priority });
});

test('Docker configuration code editor marks syntax errors and saves a JSON object', async ({ page }) => {
  await mockGraphs(page);
  let draft = report;
  await page.route(`**${api}/graphs/${report.id}/draft`, (route) => {
    if (route.request().method() === 'PUT') draft = route.request().postDataJSON().graph;
    return route.fulfill({ json: draft });
  });
  await page.goto(home);
  await draftRow(page, report.name).getByRole('button', { name: 'Edit', exact: true }).click();
  await page.getByRole('button', { name: 'Edit Docker configuration JSON' }).click();
  const configDialog = page.getByRole('dialog', { name: 'Docker configuration' });
  const editor = configDialog.getByRole('textbox', { name: 'Docker configuration JSON' });
  const apply = configDialog.getByRole('button', { name: 'Apply configuration' });
  await expect(editor).toContainText('example.invalid/worker');
  await expect(configDialog.locator('.cm-lineNumbers')).toBeVisible();
  await editor.fill('{\n  "image":\n}');
  await expect(configDialog.locator('.cm-lint-marker-error')).toBeVisible();
  await apply.click();
  await expect(configDialog.getByRole('alert')).toBeVisible();
  await editor.fill('[]');
  await apply.click();
  await expect(configDialog.getByRole('alert')).toContainText('Docker configuration must be a JSON object.');

  const config = { image: 'example.invalid/new-worker', command: ['run', '--batch'], env: ['MODE=batch'], labels: { team: 'data' } };
  await editor.fill(JSON.stringify(config, null, 2));
  await apply.click();
  await expect(configDialog).toBeHidden();
  await expect(page.getByLabel('Docker image', { exact: true })).toHaveValue(config.image);
  await expect(page.getByRole('textbox', { name: 'Command argument 2' })).toHaveValue('--batch');
  await expect(page.getByRole('textbox', { name: 'Environment variable 1' })).toHaveValue('MODE=batch');
  await page.getByRole('button', { name: 'Save draft', exact: true }).click();
  await expect(page.getByText('Draft saved.', { exact: true })).toBeVisible();
  expect(draft.nodes[0].settings.config).toEqual(config);
});

test('Node Properties saves Docker arguments and environment entries from the form', async ({ page }) => {
  await mockGraphs(page);
  const node = report.nodes[0];
  const config = { ...node.settings.config, env: ['DISABLE_FROM_IMAGE', 'MODE=old'], working_dir: '/app' };
  let draft: Graph = { ...report, nodes: [{ ...node, settings: { ...node.settings, config } }] };
  await page.route(`**${api}/graphs/${report.id}/draft`, (route) => {
    if (route.request().method() === 'PUT') draft = route.request().postDataJSON().graph;
    return route.fulfill({ json: draft });
  });
  await page.goto(home);
  await draftRow(page, report.name).getByRole('button', { name: 'Edit', exact: true }).click();

  const inspector = page.getByRole('complementary');
  const command = inspector.getByRole('group', { name: 'Command', exact: true });
  const args = ['sh', '-c', 'echo "$MODE"\nexit 0', '  with spaces  ', ''];
  for (const [index, arg] of args.entries()) {
    await command.getByRole('button', { name: 'Add argument', exact: true }).click();
    await command.getByRole('textbox', { name: `Command argument ${index + 1}` }).fill(arg);
  }
  const env = inspector.getByRole('group', { name: 'Environment variables', exact: true });
  await expect(env.getByRole('textbox', { name: 'Environment variable 1' })).toHaveValue('DISABLE_FROM_IMAGE');
  await env.getByRole('button', { name: 'Remove environment variable 2' }).click();
  await env.getByRole('button', { name: 'Add variable' }).click();
  await env.getByRole('textbox', { name: 'Environment variable 2' }).fill('MODE=region=eu');
  await env.getByRole('button', { name: 'Add variable' }).click();
  await env.getByRole('textbox', { name: 'Environment variable 3' }).fill('EMPTY=');

  await inspector.getByRole('button', { name: 'Edit Docker configuration JSON' }).click();
  const dialog = page.getByRole('dialog', { name: 'Docker configuration' });
  const editor = dialog.getByRole('textbox', { name: 'Docker configuration JSON' });
  await expect(editor).toContainText('"command"');
  await expect(editor).toContainText('MODE=region=eu');
  await dialog.getByRole('button', { name: 'Cancel' }).click();
  await page.getByRole('button', { name: 'Save draft', exact: true }).click();
  await expect(page.getByText('Draft saved.', { exact: true })).toBeVisible();
  expect(draft.nodes[0].settings.config).toEqual({ ...config, command: args, env: ['DISABLE_FROM_IMAGE', 'MODE=region=eu', 'EMPTY='] });

  for (let index = args.length; index > 0; index--) await command.getByRole('button', { name: `Remove command argument ${index}` }).click();
  await page.getByRole('button', { name: 'Save draft', exact: true }).click();
  await expect(page.getByText('All changes saved', { exact: true })).toBeVisible();
  expect(draft.nodes[0].settings.config).toEqual({ ...config, env: ['DISABLE_FROM_IMAGE', 'MODE=region=eu', 'EMPTY='] });
});

test('The draft editor saves output kinds and converts legacy multi-file JSON', async ({ page }) => {
  await mockGraphs(page);
  let draft = report;
  await page.route(`**${api}/graphs/${report.id}/draft`, (route) => {
    if (route.request().method() === 'PUT') draft = route.request().postDataJSON().graph;
    return route.fulfill({ json: draft });
  });
  await page.goto(home);
  await draftRow(page, report.name).getByRole('button', { name: 'Edit', exact: true }).click();

  await page.getByRole('button', { name: 'Edit JSON' }).click();
  const editor = page.getByRole('dialog', { name: 'Workflow JSON' });
  const [node] = draft.nodes;
  await editor.getByRole('textbox', { name: 'Workflow JSON' }).fill(JSON.stringify({
    ...draft,
    nodes: [{
      ...node,
      outputs: [
        { file_name: 'bundle', kind: 'File', multiple: true },
        { file_name: 'report.json', kind: 'File' },
      ],
    }],
  }));
  await editor.getByRole('button', { name: 'Apply changes' }).click();

  const inspector = page.getByRole('complementary');
  await expect(inspector.getByRole('checkbox', { name: 'Multiple files' })).toHaveCount(0);
  await expect(inspector.getByRole('combobox', { name: 'Output 1 kind' })).toHaveValue('Directory');
  await expect(inspector.getByRole('combobox', { name: 'Output 2 kind' })).toHaveValue('File');
  await page.getByRole('button', { name: 'Save draft', exact: true }).click();
  await expect(page.getByText('Draft saved.', { exact: true })).toBeVisible();
  expect(draft.nodes[0].outputs).toEqual([
    { file_name: 'bundle', kind: 'Directory' },
    { file_name: 'report.json', kind: 'File' },
  ]);
});

test('The browser uploads a shared asset to RustFS and pins it on a node', async ({ page }) => {
  await mockGraphs(page);
  const script = Buffer.from('print("hello")');
  const asset: AssetVersion = {
    asset_id: 'aa000000-0000-4000-8000-000000000001',
    name: 'transform-v1_example.lua', version: 1, size: script.length, format: 'text/x-lua',
    sha256: createHash('sha256').update(script).digest('hex'),
    created_by: 'local', created_at: '2026-11-15T02:29:00Z', published_at: '2026-11-15T02:30:00Z',
  };
  const uploadId = 'aa000000-0000-4000-8000-000000000002';
  const uploadUrl = 'https://rustfs.example.test/bucket/asset-staging/upload?signature=fixture';
  const downloadUrl = 'https://rustfs.example.test/bucket/assets/published?signature=attachment';
  const expiresAt = '2030-01-01T00:00:00Z';
  const catalog: Catalog = {
    id: 2, uuid: 'aa000000-0000-4000-8000-000000000003', key: '/assets/published',
    url: 's3://bucket/assets/published', size: asset.size, format: asset.format, uploaded: true,
    created_at: asset.published_at, updated_at: asset.published_at,
  };
  let abandoned = true;
  let committed = false;
  let uploaded = false;
  let draft = report;
  await page.route(`**${api}/assets?limit=50`, (route) => route.fulfill({
    json: { items: committed ? [asset] : [], next_cursor: null },
  }));
  await page.route(`**${api}/assets/${asset.asset_id}`, (route) => route.fulfill({ json: asset }));
  await page.route(`**${api}/assets/prepare`, (route) => {
    expect(route.request().postDataJSON()).toEqual({
      name: asset.name, version: asset.version, size: asset.size, format: asset.format, sha256: asset.sha256,
    });
    if (abandoned) return route.fulfill({ status: 409, json: { error: 'The asset version is already reserved.' } });
    return route.fulfill({ json: {
      state: 'pending', asset_id: asset.asset_id, upload_id: uploadId,
      catalog: {
        ...catalog, id: 1, uuid: 'aa000000-0000-4000-8000-000000000004', key: '/asset-staging/upload',
        url: 's3://bucket/asset-staging/upload', uploaded: false,
        created_at: asset.created_at, updated_at: asset.created_at,
      },
      upload: { operation: 'upload', url: uploadUrl, headers: { 'x-upload-marker': 'staging' }, expires_at: expiresAt },
    } satisfies PrepareAssetResponse });
  });
  await page.route(`**${api}/assets/${asset.name}/versions/${asset.version}/cancel`, (route) => {
    expect(route.request().method()).toBe('POST');
    expect(route.request().postDataJSON()).toEqual({});
    abandoned = false;
    return route.fulfill({ status: 204 });
  });
  await page.route(uploadUrl, (route) => {
    const request = route.request();
    const headers = { 'access-control-allow-origin': '*', 'access-control-allow-methods': 'PUT', 'access-control-allow-headers': '*' };
    if (request.method() === 'OPTIONS') return route.fulfill({ status: 204, headers });
    expect(request.method()).toBe('PUT');
    expect(request.headers()['content-type']).toBe(asset.format);
    expect(request.headers()['x-upload-marker']).toBe('staging');
    expect(request.headers()['x-rayflow-request']).toBeUndefined();
    expect(request.postDataBuffer()).toEqual(script);
    uploaded = true;
    return route.fulfill({ status: 200, headers });
  });
  await page.route(`**${api}/assets/${asset.asset_id}/commit`, (route) => {
    expect(uploaded).toBe(true);
    expect(route.request().postDataJSON()).toEqual({ upload_id: uploadId });
    committed = true;
    return route.fulfill({ json: { ...asset, catalog } satisfies PublishedAsset });
  });
  await page.route(`**${api}/assets/${asset.asset_id}/download`, (route) => route.fulfill({
    json: {
      asset_id: asset.asset_id, sha256: asset.sha256, catalog, multipart: false,
      presigned: { operation: 'download', url: downloadUrl, headers: {}, expires_at: expiresAt },
    } satisfies AssetTransfer,
  }));
  await page.context().route(downloadUrl, (route) => route.fulfill({
    headers: { 'content-type': 'text/html', 'content-disposition': `attachment; filename="${asset.name}"` },
    body: script,
  }));
  await page.route(`**${api}/graphs/${report.id}/draft`, (route) => {
    if (route.request().method() === 'PUT') draft = route.request().postDataJSON().graph;
    return route.fulfill({ json: draft });
  });

  await page.goto('/#/assets');
  await page.getByRole('button', { name: 'Upload asset' }).click();
  const upload = page.getByRole('dialog', { name: 'Upload asset' });
  const assetName = upload.getByLabel('Asset name');
  await assetName.fill('scripts/transform.lua');
  expect(await assetName.evaluate((input: HTMLInputElement) => input.validity.patternMismatch)).toBe(true);
  await assetName.fill(asset.name);
  await upload.getByLabel('File').setInputFiles({ name: 'script.lua', mimeType: asset.format, buffer: script });
  await upload.getByRole('button', { name: 'Publish asset' }).click();
  await expect(upload.getByRole('alert')).toContainText('The asset version is already reserved.');
  await upload.getByRole('button', { name: 'Discard pending upload' }).click();
  await expect(page.getByText('Pending upload discarded.', { exact: true })).toBeVisible();
  await upload.getByRole('button', { name: 'Publish asset' }).click();
  await expect(page.getByRole('table', { name: 'Published assets' })).toContainText(asset.name);
  const [download] = await Promise.all([
    page.waitForEvent('download'),
    page.getByRole('button', { name: `Download ${asset.name} v${asset.version}` }).click(),
  ]);
  expect(download.suggestedFilename()).toBe(asset.name);

  await page.getByRole('link', { name: /^Orchestration/ }).click();
  await draftRow(page, report.name).getByRole('button', { name: 'Edit', exact: true }).click();
  await page.getByRole('button', { name: 'Add asset', exact: true }).click();
  const picker = page.getByRole('dialog', { name: 'Add asset' });
  await picker.getByLabel('Asset version').selectOption(asset.asset_id);
  await picker.getByLabel('Mount path').fill('scripts/transform.lua');
  await expect(picker.getByText('/data/assets/scripts/transform.lua', { exact: true })).toBeVisible();
  await picker.getByRole('button', { name: 'Add asset' }).click();
  await page.getByRole('button', { name: 'Save draft', exact: true }).click();
  await expect(page.getByText('Draft saved.', { exact: true })).toBeVisible();
  expect(draft.nodes[0].assets).toEqual([{ asset_id: asset.asset_id, file_name: 'scripts/transform.lua' }]);
  expect(draft.edges).toEqual([]);
});

test('The draft table shows workflow metadata, row actions, and copyable IDs', async ({ page, context }) => {
  await context.grantPermissions(['clipboard-read', 'clipboard-write']);
  await mockGraphs(page);
  await page.goto(home);

  const table = draftTable(page);
  const ingestRow = draftRow(page, ingest.name);
  await expect(table.getByRole('columnheader')).toHaveText(['Workflow', 'Nodes', 'Connections', ...auditLabels, 'Actions']);
  await expect(table.getByRole('row').filter({ has: page.getByRole('cell') })).toHaveCount(3);
  await expect(ingestRow.getByTitle(ingest.id, { exact: true })).toHaveText(ingest.id.slice(0, 6));
  await expect(ingestRow.getByRole('cell', { name: '3 nodes', exact: true })).toBeVisible();
  await expect(ingestRow.getByRole('cell', { name: '2 connections', exact: true })).toBeVisible();
  await expect(ingestRow.getByText(ingest.created_by, { exact: true })).toBeVisible();
  await expect(ingestRow.getByText(ingest.updated_by, { exact: true })).toBeVisible();
  await expect(ingestRow.locator('time').nth(0)).toHaveAttribute('datetime', ingest.created_at);
  await expect(ingestRow.locator('time').nth(1)).toHaveAttribute('datetime', ingest.updated_at);
  await expectAuditTimesOnOneLine(ingestRow);
  await expectAuditIcons(table);
  await expect(ingestRow.getByRole('cell').last().getByRole('button')).toHaveText(['Edit', 'Versions', 'Runtime']);
  const reportRow = draftRow(page, report.name);
  await expect(reportRow.getByText('1 node', { exact: true })).toBeVisible();
  await expect(reportRow.getByText('—', { exact: true })).toHaveCount(4);
  await expect(draftRow(page, telemetry.name)).toBeVisible();
  await expect(table).not.toContainText(/last run|run duration|success rate/i);

  await ingestRow.getByRole('button', { name: 'Copy workflow ID' }).click();
  await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe(ingest.id);
  await page.getByRole('button', { name: 'Dismiss message' }).click();

  await page.evaluate(() => {
    navigator.clipboard.writeText = async () => { throw new DOMException('Clipboard access denied', 'NotAllowedError'); };
    document.execCommand = () => false;
  });
  await reportRow.getByRole('button', { name: 'Copy workflow ID' }).click();
  const manualCopy = page.getByRole('dialog', { name: 'Copy workflow ID' });
  const uuid = manualCopy.getByRole('textbox', { name: 'Workflow ID' });
  await expect(uuid).toHaveValue(report.id);
  await expect.poll(() => uuid.evaluate((field: HTMLInputElement) => field.value.slice(field.selectionStart ?? 0, field.selectionEnd ?? 0))).toBe(report.id);
  await manualCopy.getByRole('button', { name: 'Done' }).click();
});

test('Audit names resolve historical users while preserving the stored identities', async ({ page }) => {
  const reviewer = { id: 'b'.repeat(64), name: 'Reviewer', provider: 'local', subject: 'reviewer' };
  const adminId = 'a'.repeat(64);
  const publisherId = 'c'.repeat(64);
  const unknownId = 'd'.repeat(64);
  const names: Record<string, string> = { [adminId]: 'admin', [publisherId]: 'Release operator' };
  const draft = { ...ingest, created_by: adminId, updated_by: reviewer.id };
  const otherDraft = { ...report, created_by: adminId, updated_by: unknownId };
  const published = { ...ingestVersion, created_by: adminId, updated_by: publisherId };
  await mockGraphs(page);
  await page.route(`**${api}/auth/providers`, (route) => route.fulfill({ json: { enabled: true, providers: [] } }));
  await page.route(`**${api}/auth/me`, (route) => route.fulfill({ json: reviewer }));
  await page.route(`**${api}/graph_drafts?limit=50`, (route) => route.fulfill({ json: { items: [draft, otherDraft], next_cursor: null } }));
  await page.route(`**${api}/graphs?limit=50`, (route) => route.fulfill({ json: { items: [published], next_cursor: null } }));
  const draftPath = `**${api}/graphs/${draft.id}/draft`;
  await page.route(draftPath, (route) => route.fulfill({ json: draft }));
  const batches: { method: string; ids: string[] }[] = [];
  await page.route(`**${api}/users/resolve`, (route) => {
    const request = route.request();
    const { ids }: { ids: string[] } = request.postDataJSON();
    batches.push({ method: request.method(), ids });
    return route.fulfill({ json: Object.fromEntries(ids.filter((id) => id in names).map((id) => [id, names[id]])) });
  });
  await page.goto(home);

  const selectedDraft = draftRow(page, draft.name);
  await expect(selectedDraft.getByText('admin', { exact: true })).toBeVisible();
  await expect(selectedDraft.getByText('admin', { exact: true })).toHaveAttribute('title', `admin (${adminId})`);
  await expect(selectedDraft.getByText(reviewer.name, { exact: true })).toBeVisible();
  await expect(draftRow(page, otherDraft.name).getByText('admin', { exact: true })).toBeVisible();
  await expect(draftRow(page, otherDraft.name).getByText(unknownId, { exact: true })).toBeVisible();
  expect(batches).toEqual([{ method: 'POST', ids: [adminId, unknownId] }]);

  await selectedDraft.getByRole('button', { name: 'Edit', exact: true }).click();
  await expect(page.getByRole('textbox', { name: 'Workflow name' })).toHaveValue(draft.name);
  const [save] = await Promise.all([
    page.waitForRequest(draftPath),
    page.getByRole('button', { name: 'Save draft' }).click(),
  ]);
  expect(save.method()).toBe('PUT');
  expect(save.postDataJSON()).toMatchObject({ actor: reviewer.id, graph: draft });
  await expect(page.getByText('Draft saved.', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Back to workflows' }).click();
  await page.getByRole('button', { name: 'Published versions', exact: true }).click();
  const row = publishedRow(page, published);
  await expect(row.getByRole('cell', { name: 'admin', exact: true })).toBeVisible();
  await expect(row.getByRole('cell', { name: 'Release operator', exact: true })).toBeVisible();
  await expect(row.getByTitle(published.id, { exact: true })).toHaveText(published.id.slice(0, 6));
});

test('Search, sorting, and favorites apply to the current page', async ({ page }) => {
  await mockGraphs(page);
  await page.goto(home);

  const titles = draftTable(page).locator('.table-name');
  const sort = page.getByRole('combobox', { name: 'Sort workflows on this page' });
  const search = page.getByRole('textbox', { name: 'Search this page…' });
  const favorites = page.getByRole('button', { name: 'My favorites on this page' });
  const ingestFavorite = draftRow(page, ingest.name).getByRole('button', { name: 'Favorite workflow' });

  await expect(titles).toHaveText([ingest.name, report.name, telemetry.name]);
  await sort.selectOption({ label: 'Name A–Z' });
  await expect(titles).toHaveText([report.name, telemetry.name, ingest.name]);
  await sort.selectOption({ label: 'Name Z–A' });
  await expect(titles).toHaveText([ingest.name, telemetry.name, report.name]);

  await search.fill('dAtA iNgEsT');
  await expect(titles).toHaveText([ingest.name]);
  await search.fill(report.id);
  await expect(titles).toHaveText([report.name]);
  await search.fill(history.name);
  await expect(page.getByRole('heading', { name: 'No matching workflows' })).toBeVisible();
  const nextPage = page.getByRole('button', { name: 'Next page' });
  const [request] = await Promise.all([page.waitForRequest(`**${api}/graph_drafts?limit=50&before=${cursor}`), nextPage.click()]);
  expect(request.method()).toBe('GET');
  await expect(titles).toHaveText([history.name]);
  await expect(page.getByText('Page 2', { exact: true })).toBeVisible();
  await expect(nextPage).toBeDisabled();

  await page.getByRole('button', { name: 'Previous page' }).click();
  await search.clear();
  await expect(ingestFavorite).toHaveAttribute('aria-pressed', 'false');
  await ingestFavorite.click();
  await expect(ingestFavorite.locator('svg')).toHaveCSS('fill', 'none');
  await favorites.click();
  await expect(titles).toHaveText([ingest.name]);
  await page.reload();
  await expect(ingestFavorite).toHaveAttribute('aria-pressed', 'true');
  if (await favorites.getAttribute('aria-pressed') === 'false') await favorites.click();
  await expect(titles).toHaveText([ingest.name]);
  await ingestFavorite.click();
  await expect(page.getByRole('heading', { name: 'No favorite workflows on this page' })).toBeVisible();
});

test('Draft rows open the selected workflow and keep New workflow available', async ({ page }) => {
  await mockGraphs(page);
  const draftPath = `**${api}/graphs/${report.id}/draft`;
  await page.route(draftPath, (route) => route.fulfill({ json: report }));
  await page.goto(home);

  const [request] = await Promise.all([
    page.waitForRequest(draftPath),
    draftRow(page, report.name).getByRole('button', { name: 'Edit', exact: true }).click(),
  ]);
  expect(request.method()).toBe('GET');
  await expect(page.getByRole('textbox', { name: 'Workflow name' })).toHaveValue(report.name);
  await expect(page.getByTestId('flow-canvas').getByText(report.nodes[0].name, { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Back to workflows' }).click();

  await page.getByRole('button', { name: 'New workflow' }).click();
  const create = page.getByRole('dialog', { name: 'Create a workflow' });
  const desktop = page.viewportSize()!;
  const workflowName = create.getByLabel('Workflow name');
  await expectModalInputUncovered(workflowName);
  await page.setViewportSize({ width: 390, height: 844 });
  await expectModalInputUncovered(workflowName);
  await page.setViewportSize({ width: 390, height: 210 });
  await workflowName.focus();
  await workflowName.evaluate((element) => element.scrollIntoView({ block: 'center' }));
  await expectModalInputUncovered(workflowName);
  const footer = create.locator('.modal-actions');
  await expect(footer).toHaveCSS('position', 'sticky');
  const footerVisibility = await footer.evaluate((actions) => {
    const dialog = actions.closest('dialog')!;
    const dialogBounds = dialog.getBoundingClientRect();
    const fits = () => {
      const bounds = actions.getBoundingClientRect();
      return bounds.top >= dialogBounds.top && bounds.bottom <= dialogBounds.bottom;
    };
    dialog.scrollTop = 0;
    const atTop = fits();
    dialog.scrollTop = dialog.scrollHeight;
    return { scrolled: dialog.scrollTop > 0, atTop, atBottom: fits() };
  });
  expect(footerVisibility).toEqual({ scrolled: true, atTop: true, atBottom: true });
  await page.setViewportSize(desktop);
  await workflowName.fill('New weekly digest');
  await create.getByRole('button', { name: 'Create workflow', exact: true }).click();
  await expect(page.getByRole('textbox', { name: 'Workflow name' })).toHaveValue('New weekly digest');
  await expect(page.getByTestId('flow-canvas').getByText('start', { exact: true })).toBeVisible();
});

test('Published lists each version and Edit starts a draft from the selected version', async ({ page }) => {
  await mockGraphs(page);
  const ingestVersions = `**${api}/graphs?graph_id=${ingest.id}&limit=50`;
  await page.route(ingestVersions, (route) => route.fulfill({ json: { items: [ingestVersion], next_cursor: null } }));
  const draftPath = `**${api}/graphs/${cleanup.id}/draft`;
  const existingDraft = { ...cleanup, name: 'Current cleanup draft' };
  const draftRequests: string[] = [];
  await page.route(draftPath, (route) => {
    const request = route.request();
    draftRequests.push(request.method());
    return route.fulfill({ json: request.method() === 'PUT' ? request.postDataJSON().graph : existingDraft });
  });
  const versionPath = `**${api}/graphs/${olderCleanup.id}?version=${olderCleanup.version}`;
  await page.route(versionPath, (route) => route.fulfill({ json: olderCleanup }));
  await page.goto(home);

  await draftRow(page, ingest.name).getByRole('button', { name: 'Favorite workflow' }).click();
  await page.getByRole('button', { name: 'My favorites on this page' }).click();
  await expect(draftTable(page).locator('.table-name')).toHaveText([ingest.name]);

  await page.getByRole('button', { name: 'Published versions', exact: true }).click();
  const table = publishedTable(page);
  const rows = table.getByRole('row').filter({ has: page.getByRole('cell') });
  const cleanupRow = publishedRow(page, cleanup);
  const olderCleanupRow = publishedRow(page, olderCleanup);
  await expect(table.getByRole('columnheader')).toHaveText([
    'Workflow', 'Version', 'Nodes', 'Connections', ...auditLabels, 'Actions',
  ]);
  await expectAuditIcons(table);
  for (const label of auditLabels) await expect(table.getByRole('columnheader', { name: label, exact: true })).toHaveCount(1);
  await expect(rows).toHaveCount(3);
  const ingestRow = publishedRow(page, ingestVersion);
  await expect(ingestRow.getByRole('cell', { name: ingestVersion.created_by, exact: true })).toBeVisible();
  await expect(ingestRow.getByRole('cell', { name: ingestVersion.updated_by, exact: true })).toBeVisible();
  await expect(ingestRow.locator('time').nth(0)).toHaveAttribute('datetime', ingestVersion.created_at);
  await expect(ingestRow.locator('time').nth(1)).toHaveAttribute('datetime', ingestVersion.updated_at);
  await expectAuditTimesOnOneLine(ingestRow);
  await expect(cleanupRow.getByTitle(cleanup.id, { exact: true })).toHaveText(cleanup.id.slice(0, 6));
  await expect(cleanupRow.getByRole('cell', { name: '—', exact: true })).toHaveCount(4);
  await expect(olderCleanupRow.getByTitle(cleanup.id, { exact: true })).toHaveText(cleanup.id.slice(0, 6));
  await expect(olderCleanupRow.getByRole('cell').last().getByRole('button')).toHaveText(['Edit', 'Diff', 'Runtime']);
  await expect(page.getByRole('button', { name: /favorite/i })).toHaveCount(0);
  const search = page.getByRole('textbox', { name: 'Search this page…' });
  await search.fill(cleanup.id);
  await expect(rows).toHaveCount(2);
  await search.clear();

  const nextPage = page.getByRole('button', { name: 'Next page' });
  const [next] = await Promise.all([
    page.waitForRequest(`**${api}/graphs?limit=50&before=${publishedCursor}`),
    nextPage.click(),
  ]);
  expect(next.method()).toBe('GET');
  await expect(publishedRow(page, firstCleanup)).toBeVisible();
  await expect(rows).toHaveCount(1);
  await expect(nextPage).toBeDisabled();
  await page.getByRole('button', { name: 'Previous page' }).click();
  await expect(rows).toHaveCount(3);

  await page.getByRole('button', { name: 'Drafts', exact: true }).click();
  await draftRow(page, ingest.name).getByRole('button', { name: 'Versions', exact: true }).click();
  await expect(rows).toHaveCount(1);
  await expect(publishedRow(page, ingestVersion)).toBeVisible();

  await page.getByRole('button', { name: 'Clear workflow filter' }).click();
  await expect(rows).toHaveCount(3);
  await expect(cleanupRow.getByRole('cell', { name: '1 node', exact: true })).toBeVisible();
  await expect(olderCleanupRow.getByRole('cell', { name: '2 nodes', exact: true })).toBeVisible();
  await expect(olderCleanupRow.getByRole('cell', { name: '1 connection', exact: true })).toBeVisible();

  const [published] = await Promise.all([
    page.waitForRequest(versionPath),
    olderCleanupRow.getByRole('button', { name: 'Edit', exact: true }).click(),
  ]);
  expect(published.method()).toBe('GET');
  await expect(page.getByRole('textbox', { name: 'Workflow name' })).toHaveValue(olderCleanup.name);
  await expect(page.getByTestId('flow-canvas').getByText(olderCleanup.nodes[0].name, { exact: true })).toBeVisible();
  await expect(page.getByText('Unsaved changes', { exact: true })).toBeVisible();
  expect(draftRequests).toEqual([]);

  const [save] = await Promise.all([
    page.waitForRequest(draftPath),
    page.getByRole('button', { name: 'Save draft' }).click(),
  ]);
  expect(save.method()).toBe('PUT');
  expect(save.postDataJSON()).toMatchObject({ graph: olderCleanup });
  await expect(page.getByText('All changes saved', { exact: true })).toBeVisible();
  expect(draftRequests).toEqual(['PUT']);
  await page.getByRole('button', { name: 'Dismiss message' }).click();
  await page.getByRole('button', { name: 'Back to workflows' }).click();
  await expect(rows).toHaveCount(3);
  await expect(olderCleanupRow.getByRole('cell', { name: '2 nodes', exact: true })).toBeVisible();
});

test('Diff shows node and connection changes on the canvas and their before and after values', async ({ page }) => {
  await mockGraphs(page);
  const [load, plan, review, obsolete, archive] = graph(cleanup.id, cleanup.name, 0, [
    'Load inventory', 'List candidates', 'Review candidates', 'Remove expired records', 'Clean archive',
  ]).nodes;
  const inventory: GraphNode = { ...load, outputs: [{ file_name: 'inventory.json', kind: 'File' }] };
  const beforePlan: GraphNode = {
    ...plan,
    settings: {
      config: { image: 'alpine' }, resource: { cpu_cores: 1, memory_bytes: 512 }, timeout: '10s',
      overrides: { fast: { timeout: '5s' } },
    },
    inputs: [{ file_name: 'inventory.json', source: 'Load inventory.inventory.json', required: true, format: null }],
  };
  const afterPlan: GraphNode = {
    ...beforePlan,
    name: 'Plan cleanup',
    settings: { config: { image: 'alpine', command: ['run'] }, resource: { cpu_cores: 2, memory_bytes: 1024, gpu_memory_bytes: 4294967296, gpu_cdi_device: 'nvidia.com/gpu=1' }, timeout: '20s' },
    inputs: [{ ...beforePlan.inputs[0], required: false }],
    outputs: [{ file_name: 'summary.txt', kind: 'File' }],
  };
  const before: Graph = {
    ...olderCleanup, node_count: 4, nodes: [inventory, beforePlan, review, obsolete],
    edges: [[load.id, plan.id], [plan.id, review.id], [plan.id, obsolete.id]],
  };
  const after: Graph = {
    ...cleanup, node_count: 4, nodes: [inventory, afterPlan, review, archive],
    edges: [[load.id, plan.id], [load.id, review.id], [review.id, plan.id], [plan.id, archive.id]],
  };
  const versions = `**${api}/graphs?graph_id=${cleanup.id}&limit=50`;
  await page.route(`**${api}/graphs?limit=50`, (route) => route.fulfill({ json: { items: [after], next_cursor: null } }));
  await page.route(versions, (route) => route.fulfill({ json: { items: [after], next_cursor: publishedCursor } }));
  await page.route(`${versions}&before=${publishedCursor}`, (route) => route.fulfill({ json: { items: [before], next_cursor: null } }));
  const path = `**${api}/graphs/${after.id}/diff?from_version=${before.version}&version=${after.version}`;
  const requests: string[] = [];
  await page.route(path, (route) => {
    requests.push(route.request().method());
    return route.fulfill({ json: {
      before_node_names: Object.fromEntries(before.nodes.map(({ id, name }) => [id, name])),
      after_node_names: Object.fromEntries(after.nodes.map(({ id, name }) => [id, name])),
      changes: [
        { field: 'version', change: [2, 4] },
        { field: 'structure', change: {
          node_diffs: [[plan.id, [
            { field: 'name', change: [beforePlan.name, afterPlan.name] },
            { field: 'settings', change: [
              { field: 'config', change: [beforePlan.settings.config, afterPlan.settings.config] },
              { field: 'resource', change: [
                { field: 'cpu_cores', change: [1, 2] },
                { field: 'memory_bytes', change: [512, 1024] },
                { field: 'gpu_memory_bytes', change: [null, 4294967296] },
                { field: 'gpu_cdi_device', change: [null, 'nvidia.com/gpu=1'] },
              ] },
              { field: 'timeout', change: ['10s', '20s'] },
              { field: 'overrides', change: [
                { name: 'fast', before: { timeout: '5s' }, after: null },
              ] },
            ] },
            { field: 'inputs', change: [{ kind: 'changed', index: 0, changes: [{ field: 'required', change: [true, false] }] }] },
            { field: 'outputs', change: [{ kind: 'added', index: 0, value: afterPlan.outputs[0] }] },
          ]]],
          node_miss: [archive.id],
          node_over: [obsolete.id],
          edge_miss: [[load.id, review.id], [review.id, plan.id], [plan.id, archive.id]],
          edge_over: [[plan.id, review.id], [plan.id, obsolete.id]],
        } },
      ],
    } });
  });
  await page.goto(home);
  await page.getByRole('button', { name: 'Published versions', exact: true }).click();
  await publishedRow(page, after).getByRole('button', { name: 'Diff', exact: true }).click();

  const dialog = page.getByRole('dialog', { name: 'Compare graph versions' });
  await expect(dialog.getByText(`v${after.version}`, { exact: true })).toBeVisible();
  const compare = dialog.getByRole('button', { name: 'Compare', exact: true });
  await expect(compare).toBeDisabled();
  await dialog.getByRole('button', { name: 'Next page' }).click();
  await dialog.getByRole('combobox', { name: 'Base version' }).selectOption(`${before.id}:${before.version}`);
  expect(requests).toEqual([]);
  await compare.click();

  const canvas = dialog.getByRole('region', { name: 'Graph comparison canvas' });
  const modifiedNode = canvas.getByRole('group', { name: 'Plan cleanup, Modified', exact: true });
  const addedNode = canvas.getByRole('group', { name: 'Clean archive, Added', exact: true });
  const removedNode = canvas.getByRole('group', { name: 'Remove expired records, Removed', exact: true });
  const unchangedNode = canvas.getByRole('group', { name: 'Load inventory, Unchanged', exact: true });
  await expect(modifiedNode.getByText('Modified', { exact: true })).toBeVisible();
  await expect(addedNode.getByText('Added', { exact: true })).toBeVisible();
  await expect(removedNode.getByText('Removed', { exact: true })).toBeVisible();
  await expect(removedNode.locator('.task-node')).toHaveCSS('border-top-style', 'dashed');
  await expect(unchangedNode).toBeVisible();

  const connection = (name: string) => canvas.getByRole('img', { name, exact: true }).locator('.react-flow__edge-path');
  const removedConnection = connection('Removed connection from Plan cleanup to Remove expired records');
  const reversedConnection = connection('Removed connection from Plan cleanup to Review candidates');
  const addedConnection = connection('Added connection from Review candidates to Plan cleanup');
  await expect(removedConnection).toHaveCSS('stroke-dasharray', '6px, 4px');
  await expect(reversedConnection).toHaveCSS('stroke-dasharray', '6px, 4px');
  await expect(addedConnection).toHaveCSS('stroke-dasharray', 'none');
  const pathTop = (path: Locator) => path.evaluate((element) => (element as SVGPathElement).getBBox().y);
  expect(await pathTop(reversedConnection)).toBeLessThan(await pathTop(addedConnection));

  const nodeChanges = dialog.getByRole('table', { name: 'Changes to Plan cleanup' });
  const nodes = dialog.getByRole('table', { name: 'Added and removed nodes' });
  await expect(nodes.getByRole('row', { name: /Added/ })).toContainText('Clean archive');
  await expect(nodes.getByRole('row', { name: /Removed/ })).toContainText('Remove expired records');

  await modifiedNode.click();
  await expect(nodes).toHaveCount(0);
  await expect(nodeChanges.getByRole('columnheader')).toHaveText(['Change', 'Before', 'After']);
  await expect(nodeChanges.getByRole('row', { name: 'Name List candidates Plan cleanup' }).getByRole('cell')).toHaveText(['List candidates', 'Plan cleanup']);
  const config = nodeChanges.getByRole('row', { name: /Settings · Configuration/ }).getByRole('cell');
  await expect(config.first()).toContainText('"image": "alpine"');
  await expect(config.last()).toContainText('"command":');
  await expect(nodeChanges.getByRole('row', { name: /Settings · CPU cores/ }).getByRole('cell')).toHaveText(['1', '2']);
  await expect(nodeChanges.getByRole('row', { name: /Settings · Memory/ }).getByRole('cell')).toHaveText(['512', '1024']);
  await expect(nodeChanges.getByRole('row', { name: /Settings · GPU memory/ }).getByRole('cell')).toHaveText(['—', '4294967296']);
  await expect(nodeChanges.getByRole('row', { name: /Settings · GPU CDI device/ }).getByRole('cell')).toHaveText(['—', 'nvidia.com/gpu=1']);
  await expect(nodeChanges.getByRole('row', { name: /Settings · Timeout/ }).getByRole('cell')).toHaveText(['10s', '20s']);
  const override = nodeChanges.getByRole('row', { name: /Settings · Override · fast/ }).getByRole('cell');
  await expect(override.first()).toContainText('"timeout": "5s"');
  await expect(override.last()).toHaveText('—');
  await expect(nodeChanges.getByRole('row', { name: /Input 1 · Required/ }).getByRole('cell')).toHaveText(['true', 'false']);
  await expect(nodeChanges.getByRole('row', { name: /Output 1 · Added/ })).toContainText('summary.txt');

  await removedNode.focus();
  await removedNode.press('Enter');
  await expect(nodeChanges).toHaveCount(0);
  await expect(nodes.getByRole('row', { name: /Added/ })).toHaveCount(0);
  await expect(nodes.getByRole('row', { name: /Removed/ })).toContainText('Remove expired records');
  await unchangedNode.click();
  await expect(dialog.getByRole('status')).toHaveText('Load inventory is unchanged.');
  await dialog.getByRole('button', { name: 'Show all node changes' }).click();
  await expect(nodeChanges).toBeVisible();
  await expect(nodes.getByRole('row', { name: /Added/ })).toContainText('Clean archive');
  await expect(nodes.getByRole('row', { name: /Removed/ })).toContainText('Remove expired records');
  await expect(dialog.getByRole('table', { name: 'Connection changes' }).getByRole('cell')).toHaveText([
    'List candidates → Review candidates', '—',
    'List candidates → Remove expired records', '—',
    '—', 'Load inventory → Review candidates',
    '—', 'Review candidates → Plan cleanup',
    '—', 'Plan cleanup → Clean archive',
  ]);
  await expect(dialog.getByRole('table', { name: 'Workflow field changes' }).getByRole('cell')).toHaveText(['2', '4']);
  expect(requests).toEqual(['GET']);
  await dialog.getByRole('button', { name: 'Close dialog' }).click();
  await expect(dialog).toBeHidden();
});

test('Diff can compare another workflow and retry a failed comparison', async ({ page }) => {
  await mockGraphs(page);
  const base = { ...cleanup, id: ingestVersion.id, name: ingestVersion.name };
  await page.route(`**${api}/graphs?limit=50`, (route) => route.fulfill({ json: { items: [cleanup, base], next_cursor: null } }));
  await page.route(`**${api}/graphs?graph_id=${cleanup.id}&limit=50`, (route) => route.fulfill({ json: { items: [cleanup], next_cursor: null } }));
  const path = `**${api}/graphs/${cleanup.id}/diff?from_version=${base.version}&version=${cleanup.version}&from_graph_id=${base.id}`;
  const names = Object.fromEntries(cleanup.nodes.map(({ id, name }) => [id, name]));
  let requests = 0;
  await page.route(path, (route) => {
    requests += 1;
    if (requests === 1) return route.fulfill({ status: 503, json: { error: 'Comparison unavailable' } });
    return route.fulfill({ json: {
      changes: [{ field: 'id', change: [base.id, cleanup.id] }, { field: 'name', change: [base.name, cleanup.name] }],
      before_node_names: names, after_node_names: names,
    } });
  });
  await page.goto(home);
  await page.getByRole('button', { name: 'Published versions', exact: true }).click();
  await publishedRow(page, cleanup).getByRole('button', { name: 'Diff', exact: true }).click();

  const dialog = page.getByRole('dialog', { name: 'Compare graph versions' });
  await dialog.getByRole('checkbox', { name: 'Include other workflows' }).check();
  await dialog.getByRole('combobox', { name: 'Base version' }).selectOption(`${base.id}:${base.version}`);
  await dialog.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect(dialog.getByRole('alert')).toContainText('Comparison unavailable');
  await dialog.getByRole('button', { name: 'Try again' }).click();
  await expect(dialog.getByRole('table', { name: 'Workflow field changes' }).getByRole('row', { name: /Workflow name/ }).getByRole('cell')).toHaveText([base.name, cleanup.name]);
  await expect(dialog.getByRole('group', { name: 'Remove expired records, Unchanged' })).toBeVisible();
  await expect(dialog.getByRole('status')).toHaveText('The nodes and connections match.');
  expect(requests).toBe(2);
});

test('Draft Runtime opens runs for its workflow without starting one', async ({ page }) => {
  await mockGraphs(page);
  const draft = { ...cleanup, version: 0 };
  await page.route(`**${api}/graph_drafts?limit=50`, (route) => route.fulfill({ json: { items: [ingest, draft], next_cursor: null } }));
  const run: Run = {
    id: 58,
    uuid: 'ac000001-0000-4000-8000-000000000000',
    data_id: 'ad000001-0000-4000-8000-000000000000',
    graph_id: cleanup.id,
    version: cleanup.version,
    revision: 1,
    created_at: '2026-11-18T00:00:00Z',
    updated_at: '2026-11-18T00:00:00Z',
  };
  const earlierRun = { ...run, id: 39, uuid: 'ac000002-0000-4000-8000-000000000000', version: olderCleanup.version };
  const unrelated = { ...run, id: 62, uuid: 'ac000003-0000-4000-8000-000000000000', graph_id: ingest.id, version: ingest.version };
  const runCursor = 57;
  const filtered = `**${api}/runs?graph_id=${cleanup.id}&limit=50`;
  const filteredNext = `${filtered}&before=${runCursor}`;
  await page.route(`**${api}/graphs/${cleanup.id}`, (route) => route.fulfill({ json: cleanup }));
  await page.route(`**${api}/graphs?limit=200`, (route) => route.fulfill({ json: { items: [cleanup, ingestVersion, olderCleanup], next_cursor: null } }));
  await page.route(filtered, (route) => route.fulfill({ json: { items: [run], next_cursor: runCursor } }));
  await page.route(filteredNext, (route) => route.fulfill({ json: { items: [earlierRun], next_cursor: null } }));
  await page.route(`**${api}/runs?limit=50`, (route) => route.fulfill({ json: { items: [unrelated, run, earlierRun], next_cursor: null } }));
  const mutations: string[] = [];
  const unfiltered: string[] = [];
  page.on('request', (request) => {
    const url = new URL(request.url());
    if (url.pathname.startsWith(`${api}/runs`) && request.method() !== 'GET') mutations.push(url.pathname);
    if (url.pathname === `${api}/runs` && !url.searchParams.has('graph_id')) unfiltered.push(url.search);
  });

  const inspect = (entry: Run) => page.getByRole('button', { name: `Inspect run ${entry.uuid}`, exact: true });
  await page.goto(home);
  const [filteredRequest] = await Promise.all([
    page.waitForRequest(filtered),
    draftRow(page, draft.name).getByRole('button', { name: 'Runtime', exact: true }).click(),
  ]);
  expect(filteredRequest.method()).toBe('GET');
  await expect(page).toHaveURL(/#\/runtime$/);
  await expect(page.getByRole('heading', { name: 'Runtime', exact: true })).toBeVisible();
  await expect(page.getByTitle(`${cleanup.name} · ${cleanup.id}`, { exact: true })).toBeVisible();
  await expect(inspect(run)).toBeVisible();
  await expect(inspect(unrelated)).toHaveCount(0);

  await Promise.all([
    page.waitForRequest(filteredNext),
    page.getByRole('button', { name: 'Next page' }).click(),
  ]);
  await expect(inspect(earlierRun)).toBeVisible();
  await expect(page.getByText('Page 2', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Clear selected workflow' }).click();
  await expect(page.getByRole('heading', { name: 'Choose a workflow' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Start a run' })).toBeDisabled();
  await expect(inspect(earlierRun)).toHaveCount(0);
  await expect(page.getByRole('dialog', { name: 'Start a workflow run' })).toHaveCount(0);
  expect(unfiltered).toEqual([]);
  expect(mutations).toEqual([]);
});

test('Draft and published tables scroll within a narrow viewport', async ({ page }) => {
  const width = 320;
  await page.setViewportSize({ width, height: 844 });
  await mockGraphs(page);
  await page.goto(home);

  const ingestRow = draftRow(page, ingest.name);
  await expectAuditTimesOnOneLine(ingestRow);
  const telemetryRow = draftRow(page, telemetry.name);
  await expect(telemetryRow.getByRole('button', { name: longName, exact: true })).toBeVisible();
  const draftId = telemetryRow.getByTitle(telemetry.id, { exact: true });
  await expect(draftId).toHaveText(telemetry.id.slice(0, 6));
  await expect(draftId).toBeVisible();
  expect(await draftId.evaluate((code) => code.scrollWidth <= code.clientWidth)).toBe(true);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
  const draftRecords = page.getByRole('region', { name: 'Draft workflow records' });
  expect(await draftRecords.evaluate((element) => element.scrollWidth > element.clientWidth)).toBe(true);
  await draftRecords.evaluate((element) => { element.scrollLeft = element.scrollWidth; });
  await expect(telemetryRow.getByRole('button', { name: 'Edit', exact: true })).toBeInViewport();

  await page.getByRole('button', { name: 'Published versions', exact: true }).click();
  await expect(publishedTable(page)).toBeVisible();
  const publishedIngest = publishedRow(page, ingestVersion);
  await expectAuditTimesOnOneLine(publishedIngest);
  const publishedId = publishedIngest.getByTitle(ingestVersion.id, { exact: true });
  await expect(publishedId).toHaveText(ingestVersion.id.slice(0, 6));
  await expect(publishedId).toBeVisible();
  expect(await publishedId.evaluate((code) => code.scrollWidth <= code.clientWidth)).toBe(true);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
  const publishedRecords = page.getByRole('region', { name: 'Published workflow records' });
  expect(await publishedRecords.evaluate((element) => element.scrollWidth > element.clientWidth)).toBe(true);
});
