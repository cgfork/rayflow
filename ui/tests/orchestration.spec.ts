import { expect, test, type Locator, type Page } from '@playwright/test';
import type { Graph, GraphNode, Run } from '../src/types';

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
const longName = 'ContinuousTelemetryAggregationWithAnIntentionallyUnbrokenWorkflowTitleForTheResponsiveCardGrid';
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

function workflows(page: Page) {
  return page.getByRole('list', { name: 'Workflows', exact: true });
}

function card(page: Page, name: string) {
  return workflows(page).getByRole('listitem').filter({ has: page.getByRole('heading', { name, exact: true }) });
}

async function expectIdentityBesideIcon(workflowCard: Locator, graph: Graph) {
  const [icon, title, id] = await Promise.all([
    workflowCard.locator('.workflow-card-icon').boundingBox(),
    workflowCard.getByRole('heading', { name: graph.name, exact: true }).boundingBox(),
    workflowCard.getByTitle(graph.id, { exact: true }).boundingBox(),
  ]);
  expect(icon).not.toBeNull();
  expect(title).not.toBeNull();
  expect(id).not.toBeNull();
  const iconRight = icon!.x + icon!.width;
  expect(title!.x).toBeGreaterThanOrEqual(iconRight);
  expect(id!.x).toBeGreaterThanOrEqual(iconRight);
  expect(title!.y).toBeLessThan(icon!.y + icon!.height);
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

async function expectPairedAuditRows(workflowCard: Locator) {
  const values = workflowCard.locator('.workflow-audit dd');
  await expect(values).toHaveCount(4);
  const [creator, createdAt, updater, updatedAt] = await values.evaluateAll((elements) => elements.map((element) => {
    const { x, y, height } = element.getBoundingClientRect();
    return { x, centerY: y + height / 2 };
  }));
  expect(creator.x).toBeLessThan(createdAt.x);
  expect(updater.x).toBeLessThan(updatedAt.x);
  expect(Math.abs(creator.centerY - createdAt.centerY)).toBeLessThanOrEqual(2);
  expect(Math.abs(updater.centerY - updatedAt.centerY)).toBeLessThanOrEqual(2);
  expect(updater.centerY).toBeGreaterThan(creator.centerY);
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

test('Draft cards show graph metadata and let users copy the full workflow ID', async ({ page, context }) => {
  await context.grantPermissions(['clipboard-read', 'clipboard-write']);
  await mockGraphs(page);
  await page.goto(home);

  const ingestCard = card(page, ingest.name);
  await expect(workflows(page).getByRole('listitem')).toHaveCount(3);
  await expect(ingestCard.getByText('Draft', { exact: true })).toHaveCount(0);
  await expect(ingestCard.getByTitle(ingest.id, { exact: true })).toHaveText(ingest.id.slice(0, 6));
  await expectIdentityBesideIcon(ingestCard, ingest);
  await expect(ingestCard.getByText('3 nodes', { exact: true })).toBeVisible();
  await expect(ingestCard.getByText('2 connections', { exact: true })).toBeVisible();
  await expect(ingestCard.getByText(ingest.created_by, { exact: true })).toBeVisible();
  await expect(ingestCard.getByText(ingest.updated_by, { exact: true })).toBeVisible();
  await expect(ingestCard.locator('time').nth(0)).toHaveAttribute('datetime', ingest.created_at);
  await expect(ingestCard.locator('time').nth(1)).toHaveAttribute('datetime', ingest.updated_at);
  await expectAuditTimesOnOneLine(ingestCard);
  await expectPairedAuditRows(ingestCard);
  await expectAuditIcons(ingestCard);
  expect(await ingestCard.evaluate((element) => element.getBoundingClientRect().width)).toBeLessThan(320);
  const footer = ingestCard.locator('.workflow-card-footer');
  const footerActions = footer.getByRole('button');
  await expect(footerActions).toHaveText(['Edit', 'Versions', 'Runtime']);
  const [edit, versions, runtime] = await footerActions.evaluateAll((elements) => elements.map((element) => {
    const { x, y, right } = element.getBoundingClientRect();
    return { x, y, right };
  }));
  const footerRight = await footer.evaluate((element) => element.getBoundingClientRect().right - parseFloat(getComputedStyle(element).paddingRight));
  expect(Math.abs(runtime.right - footerRight)).toBeLessThanOrEqual(1);
  expect(edit.x).toBeLessThan(versions.x);
  expect(versions.right).toBeLessThanOrEqual(runtime.x);
  expect(Math.abs(edit.y - runtime.y)).toBeLessThanOrEqual(1);
  expect(Math.abs(versions.y - runtime.y)).toBeLessThanOrEqual(1);
  const reportCard = card(page, report.name);
  await expect(reportCard.getByText('1 node', { exact: true })).toBeVisible();
  await expect(reportCard.getByText('—', { exact: true })).toHaveCount(4);
  await expect(card(page, telemetry.name).getByRole('heading', { name: longName, exact: true })).toBeVisible();
  await expect(workflows(page)).not.toContainText(/last run|run duration|success rate/i);

  await ingestCard.getByRole('button', { name: 'Copy workflow ID' }).click();
  await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe(ingest.id);
  await page.getByRole('button', { name: 'Dismiss message' }).click();

  await page.evaluate(() => {
    navigator.clipboard.writeText = async () => { throw new DOMException('Clipboard access denied', 'NotAllowedError'); };
    document.execCommand = () => false;
  });
  await reportCard.getByRole('button', { name: 'Copy workflow ID' }).click();
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

  const draftCard = card(page, draft.name);
  await expect(draftCard.getByText('admin', { exact: true })).toBeVisible();
  await expect(draftCard.getByText('admin', { exact: true })).toHaveAttribute('title', `admin (${adminId})`);
  await expect(draftCard.getByText(reviewer.name, { exact: true })).toBeVisible();
  await expect(card(page, otherDraft.name).getByText('admin', { exact: true })).toBeVisible();
  await expect(card(page, otherDraft.name).getByText(unknownId, { exact: true })).toBeVisible();
  expect(batches).toEqual([{ method: 'POST', ids: [adminId, unknownId] }]);

  await draftCard.getByRole('button', { name: 'Edit', exact: true }).click();
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

  const titles = workflows(page).getByRole('heading');
  const sort = page.getByRole('combobox', { name: 'Sort workflows on this page' });
  const search = page.getByRole('textbox', { name: 'Search this page…' });
  const favorites = page.getByRole('button', { name: 'My favorites on this page' });
  const ingestFavorite = card(page, ingest.name).getByRole('button', { name: 'Favorite workflow' });

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

test('Draft cards open the selected workflow and keep New workflow available', async ({ page }) => {
  await mockGraphs(page);
  const draftPath = `**${api}/graphs/${report.id}/draft`;
  await page.route(draftPath, (route) => route.fulfill({ json: report }));
  await page.goto(home);

  const [request] = await Promise.all([
    page.waitForRequest(draftPath),
    card(page, report.name).getByRole('button', { name: 'Edit', exact: true }).click(),
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

  await card(page, ingest.name).getByRole('button', { name: 'Favorite workflow' }).click();
  await page.getByRole('button', { name: 'My favorites on this page' }).click();
  await expect(workflows(page).getByRole('heading')).toHaveText([ingest.name]);

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
  await expect(olderCleanupRow.getByRole('cell').last().getByRole('button')).toHaveText(['Edit', 'Runtime']);
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
  await card(page, ingest.name).getByRole('button', { name: 'Versions', exact: true }).click();
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
    card(page, draft.name).getByRole('button', { name: 'Runtime', exact: true }).click(),
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

test('Draft cards and the published table fit a narrow viewport', async ({ page }) => {
  const width = 320;
  await page.setViewportSize({ width, height: 844 });
  await mockGraphs(page);
  await page.goto(home);

  const ingestCard = card(page, ingest.name);
  await expectAuditTimesOnOneLine(ingestCard);
  await expectPairedAuditRows(ingestCard);
  const telemetryCard = card(page, telemetry.name);
  await expect(telemetryCard.getByRole('heading', { name: longName, exact: true })).toBeVisible();
  const draftId = telemetryCard.getByTitle(telemetry.id, { exact: true });
  await expect(draftId).toHaveText(telemetry.id.slice(0, 6));
  await expectIdentityBesideIcon(telemetryCard, telemetry);
  await expect(draftId).toBeVisible();
  expect(await draftId.evaluate((code) => code.scrollWidth <= code.clientWidth)).toBe(true);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
  const first = await card(page, ingest.name).boundingBox();
  const last = await telemetryCard.boundingBox();
  expect(first).not.toBeNull();
  expect(last).not.toBeNull();
  expect(last!.x).toBeGreaterThanOrEqual(0);
  expect(last!.x + last!.width).toBeLessThanOrEqual(width);
  expect(last!.y).toBeGreaterThan(first!.y + first!.height);

  await page.getByRole('button', { name: 'Published versions', exact: true }).click();
  await expect(publishedTable(page)).toBeVisible();
  const ingestRow = publishedRow(page, ingestVersion);
  await expectAuditTimesOnOneLine(ingestRow);
  const publishedId = ingestRow.getByTitle(ingestVersion.id, { exact: true });
  await expect(publishedId).toHaveText(ingestVersion.id.slice(0, 6));
  await expect(publishedId).toBeVisible();
  expect(await publishedId.evaluate((code) => code.scrollWidth <= code.clientWidth)).toBe(true);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
});
