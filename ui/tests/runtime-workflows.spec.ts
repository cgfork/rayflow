import { expect, test, type Page } from '@playwright/test';
import type { Graph, Run } from '../src/types';

const first: Graph = {
  id: 'ab000001-0000-4000-8000-000000000001', name: 'Daily data pipeline', version: 4,
  node_count: 0, nodes: [], edges: [],
};
const second: Graph = { ...first, id: 'ab000002-0000-4000-8000-000000000002', name: 'Nightly reporting' };
const firstRun: Run = {
  id: 10, uuid: 'ac000001-0000-4000-8000-000000000001', graph_id: first.id,
  data_id: 'ad000001-0000-4000-8000-000000000001', version: 2, revision: 1,
  created_at: '2026-11-18T00:00:00Z', updated_at: '2026-11-18T00:00:00Z',
};
const olderRun: Run = { ...firstRun, id: 5, uuid: 'ac000005-0000-4000-8000-000000000005' };
const secondRun: Run = { ...firstRun, id: 12, uuid: 'ac000002-0000-4000-8000-000000000002', graph_id: second.id };

async function mock(page: Page) {
  await page.route('**/api/v1/**', (route) => route.fulfill({ status: 501, json: { error: 'Unexpected API request' } }));
  await page.route('**/api/v1/auth/providers', (route) => route.fulfill({ json: { enabled: false, providers: [] } }));
  await page.route('**/api/v1/health', (route) => route.fulfill({ body: 'ok' }));
  await page.route('**/api/v1/graphs?limit=200', (route) => route.fulfill({ json: { items: [first, { ...first, name: 'Previous pipeline name', version: 1 }], next_cursor: 42 } }));
  await page.route('**/api/v1/graphs?limit=200&before=42', (route) => route.fulfill({ json: { items: [second, { ...first, version: 2 }], next_cursor: null } }));
  for (const workflow of [first, second]) {
    await page.route(`**/api/v1/graphs/${workflow.id}`, (route) => route.fulfill({ json: workflow }));
  }
  await page.route(`**/api/v1/runs?graph_id=${first.id}&limit=50`, (route) => route.fulfill({ json: { items: [firstRun], next_cursor: 8 } }));
  await page.route(`**/api/v1/runs?graph_id=${first.id}&limit=50&before=8`, (route) => route.fulfill({ json: { items: [olderRun], next_cursor: null } }));
  await page.route(`**/api/v1/runs?graph_id=${second.id}&limit=50`, (route) => route.fulfill({ json: { items: [secondRun], next_cursor: null } }));
}

function inspect(page: Page, run: Run) {
  return page.getByRole('button', { name: `Inspect run ${run.uuid}`, exact: true });
}

async function choose(page: Page, workflow: Graph) {
  await page.getByRole('combobox', { name: 'Search workflows' }).fill(workflow.name);
  await page.getByRole('option', { name: new RegExp(workflow.name) }).click();
  await expect(page.locator('.workflow-picker-selected')).toContainText(workflow.name);
}

test('Runtime requires a workflow and scopes every refresh and page to the selection', async ({ page }) => {
  await mock(page);
  const requests: URL[] = [];
  page.on('request', (request) => {
    const url = new URL(request.url());
    if (url.pathname === '/api/v1/runs') requests.push(url);
  });
  await page.clock.install();
  await page.goto('/#/runtime');
  await expect(page.getByRole('heading', { name: 'Choose a workflow' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Start a run', exact: true })).toBeDisabled();
  await page.clock.fastForward(5000);
  expect(requests).toHaveLength(0);

  const search = page.getByRole('combobox', { name: 'Search workflows' });
  const options = page.getByRole('listbox', { name: 'Published workflows', exact: true });
  await search.click();
  await expect(options.getByRole('option')).toHaveCount(2);
  await expect(options.getByRole('option', { name: /Previous pipeline name/ })).toHaveCount(0);
  await choose(page, first);
  await expect(inspect(page, firstRun)).toBeVisible();
  await expect(inspect(page, secondRun)).toHaveCount(0);
  await page.clock.fastForward(20_000);
  expect(requests).toHaveLength(1);
  await Promise.all([
    page.waitForResponse(`**/api/v1/runs?graph_id=${first.id}&limit=50`),
    page.getByRole('button', { name: 'Refresh', exact: true }).click(),
  ]);
  expect(requests).toHaveLength(2);
  await page.getByRole('button', { name: 'Next page' }).click();
  await expect(inspect(page, olderRun)).toBeVisible();
  await expect(page.getByText('Page 2', { exact: true })).toBeVisible();

  await page.getByRole('textbox', { name: 'Find a run or data ID…' }).fill('no matching run');
  await choose(page, second);
  await expect(inspect(page, secondRun)).toBeVisible();
  await expect(inspect(page, olderRun)).toHaveCount(0);
  await expect(page.getByText('Page 1', { exact: true })).toBeVisible();
  await expect(page.getByRole('textbox', { name: 'Find a run or data ID…' })).toHaveValue('');
  expect(requests.every((url) => url.searchParams.has('graph_id'))).toBe(true);
  expect(requests.some((url) => url.searchParams.get('graph_id') === first.id && url.searchParams.get('before') === '8')).toBe(true);
  expect(requests.filter((url) => url.searchParams.get('graph_id') === second.id).every((url) => !url.searchParams.has('before'))).toBe(true);

  await page.getByRole('button', { name: 'Clear selected workflow' }).click();
  await expect(page.getByRole('heading', { name: 'Choose a workflow' })).toBeVisible();
  await expect(inspect(page, secondRun)).toHaveCount(0);
  const count = requests.length;
  await page.clock.fastForward(10000);
  expect(requests).toHaveLength(count);
});

test('Workflow search supports keyboard selection, ID lookup, and a scoped start command', async ({ page }) => {
  await mock(page);
  let startBody: unknown;
  await page.route('**/api/v1/runs/start', (route) => {
    startBody = route.request().postDataJSON();
    return route.fulfill({ status: 202, json: { id: 'command-id' } });
  });
  await page.goto('/#/runtime');
  const search = page.getByRole('combobox', { name: 'Search workflows' });
  const options = page.getByRole('listbox', { name: 'Published workflows', exact: true }).getByRole('option');
  const start = page.getByRole('button', { name: 'Start a run', exact: true });
  await expect(search).toBeVisible();
  await expect(page.getByText('Workflow', { exact: true })).toBeHidden();
  await search.fill(second.id.toUpperCase());
  await expect(options).toHaveCount(1);
  await search.press('Escape');
  await expect(search).toHaveAttribute('aria-expanded', 'false');
  await expect(page.getByRole('heading', { name: 'Choose a workflow' })).toBeVisible();
  await search.fill(second.name.toUpperCase());
  await expect(options).toHaveCount(1);
  await expect(options).toHaveCSS('outline-style', 'solid');
  await search.press('Enter');
  await expect(page.locator('.workflow-picker-selected')).toContainText(second.name);
  await expect(inspect(page, secondRun)).toBeVisible();
  const [pickerBounds, startBounds] = await Promise.all([
    page.locator('.workflow-picker-control').boundingBox(),
    start.boundingBox(),
  ]);
  expect(pickerBounds).not.toBeNull();
  expect(startBounds).not.toBeNull();
  expect(Math.abs(pickerBounds!.y - startBounds!.y)).toBeLessThanOrEqual(1);
  expect(Math.abs(pickerBounds!.height - startBounds!.height)).toBeLessThanOrEqual(1);
  expect(pickerBounds!.x + pickerBounds!.width).toBeLessThanOrEqual(startBounds!.x);
  await start.click();
  const dialog = page.getByRole('dialog', { name: 'Start a workflow run' });
  await expect(dialog.getByRole('textbox', { name: 'Workflow', exact: true })).toHaveValue(second.name);
  await expect(dialog.getByRole('textbox', { name: 'Workflow', exact: true })).toHaveAttribute('readonly', '');
  const dataId = await dialog.getByRole('textbox', { name: 'Data ID' }).inputValue();
  expect(dataId).toMatch(/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i);
  await dialog.getByRole('button', { name: 'Start run', exact: true }).click();
  await expect(dialog).toBeHidden();
  expect(startBody).toMatchObject({ graph_id: second.id, data_id: dataId, nodes: [] });
});

test('Workflow loading errors can be retried without requesting unfiltered runs', async ({ page }) => {
  await mock(page);
  let fail = true;
  await page.route('**/api/v1/graphs?limit=200', (route) => fail
    ? route.fulfill({ status: 500, json: { error: 'Unable to load workflows' } })
    : route.fulfill({ json: { items: [first], next_cursor: null } }));
  await page.goto('/#/runtime');
  await page.getByRole('combobox', { name: 'Search workflows' }).click();
  await expect(page.getByRole('alert')).toContainText('Unable to load workflows');
  fail = false;
  await page.getByRole('button', { name: 'Try again' }).click();
  await expect(page.getByRole('option', { name: new RegExp(first.name) })).toBeVisible();
  await choose(page, first);
  await expect(inspect(page, firstRun)).toBeVisible();
});

test('The workflow selector fits narrow screens and keeps the selected workflow visible', async ({ page }) => {
  const width = 320;
  await page.setViewportSize({ width, height: 844 });
  await mock(page);
  const workflow = { ...first, name: 'DailyDataPipelineWithAVeryLongWorkflowNameThatStillFitsInTheHeader' };
  await page.route('**/api/v1/graphs?limit=200', (route) => route.fulfill({ json: { items: [workflow, second], next_cursor: null } }));
  await page.route(`**/api/v1/graphs/${workflow.id}`, (route) => route.fulfill({ json: workflow }));
  await page.goto('/#/runtime');
  await choose(page, workflow);
  await expect(page.locator('.workflow-picker-selected')).toBeVisible();
  const table = page.getByRole('table');
  const runRow = table.getByRole('row').filter({ has: inspect(page, firstRun) });
  for (const [label, value] of [['Run', firstRun.uuid], ['Data', firstRun.data_id]]) {
    const id = runRow.getByTitle(value, { exact: true });
    await expect(id).toHaveText(`${label} ${value.slice(0, 6)}`);
    await expect(id).toBeVisible();
    expect(await id.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
  }
  expect(await runRow.evaluate((element) => element.getBoundingClientRect().height)).toBeLessThan(120);
  const search = page.getByRole('combobox', { name: 'Search workflows' });
  await search.click();
  await expect(page.getByRole('listbox', { name: 'Published workflows' })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
  await search.press('Escape');
});
