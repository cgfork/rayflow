import { expect, test, type Locator, type Page } from '@playwright/test';
import type { Agent, Graph, GraphNode, Job, NodeStatus, Run, RunView } from '../src/types';
import { expectDrawerAlignedWithInspector } from './drawer';

const api = '/api/v1';
const timestamp = '2026-11-18T08:00:00Z';
const themes = ['light', 'dark'] as const;
type ColorScheme = typeof themes[number];

function workflow(id: string, name: string, nodeNames: string[]): Graph {
  const nodes = nodeNames.map((name, index): GraphNode => ({
    id: `${id.slice(0, 24)}${String(index + 1).padStart(12, '0')}`,
    name,
    node_type: 'docker',
    settings: { config: { image: 'example.invalid/inventory-worker' }, resource: { cpu_cores: 2, memory_bytes: 536870912 }, timeout: '2m' },
    inputs: [],
    outputs: [],
  }));
  return {
    id, name, version: 0, nodes, node_count: nodes.length,
    edges: nodes.slice(1).map((node, index) => [nodes[index].id, node.id]),
    created_by: 'Maya Chen', created_at: '2026-11-15T09:20:00Z',
    updated_by: 'Leo Park', updated_at: timestamp,
  };
}

const inventory = workflow('a1000001-0000-4000-8000-000000000000', 'Daily inventory reconciliation', [
  'Collect inventory snapshots', 'Validate warehouse counts', 'Publish reconciliation report',
]);
const catalog = workflow('a1000002-0000-4000-8000-000000000000', 'Refresh warehouse catalog', ['Fetch catalog', 'Normalize product data']);
const credits = workflow('a1000003-0000-4000-8000-000000000000', 'Review partner credit memo exceptions', ['Collect submissions']);
const published = { ...inventory, version: 3 };
const versions = [published, { ...inventory, version: 2 }, { ...catalog, version: 1 }];
const run: Run = {
  id: 12, uuid: 'a2000001-0000-4000-8000-000000000000', data_id: 'a2000002-0000-4000-8000-000000000000',
  graph_id: inventory.id, version: published.version, revision: 1, created_at: timestamp, updated_at: timestamp,
};
const statuses: NodeStatus[] = ['Succeeded', 'Running', 'Ready'];
const inputFiles = [
  { file_name: 'warehouse counts.csv', source: 'collect.snapshot.csv', required: true, format: 'csv' },
  { file_name: 'warehouse manifest.json', source: 'collect.manifest.json', required: true, format: 'json' },
];
const outputFiles = [
  { file_name: 'validated warehouse counts.csv', kind: 'File' as const },
  { file_name: 'reconciliation manifest.json', kind: 'File' as const },
];
const nodesWithFiles = [{ ...published.nodes[0], outputs: outputFiles }, { ...published.nodes[1], inputs: inputFiles }, published.nodes[2]];
const runView: RunView = {
  run, graph: published,
  node_runs: nodesWithFiles.map((node, index) => [{
    ...run, id: index + 1, uuid: `a2000003-0000-4000-8000-${String(index + 1).padStart(12, '0')}`,
    run_id: run.uuid, node_id: node.id, status: statuses[index], ineffective: 0,
  }, node]),
};
const agent: Agent = {
  id: 1, uuid: 'a3000001-0000-4000-8000-000000000000', online: true,
  cpu_cores: 16, memory_bytes: 34359738368, active_jobs: 1,
  first_seen_at: timestamp, last_seen_at: timestamp, online_until: '2026-11-18T08:30:00Z',
};
const job: Job = {
  id: 1, uuid: 'a4000001-0000-4000-8000-000000000000', task_id: 'a4000002-0000-4000-8000-000000000000',
  run_id: run.uuid, node_run_id: 2, agent_id: agent.uuid, status: 'Polled',
  required_cpu_cores: 2, required_memory_bytes: 536870912, created_at: timestamp, updated_at: timestamp,
  polled_at: timestamp, expired_at: '2026-11-18T08:15:00Z',
};

async function mockWorkspace(page: Page) {
  await page.route('**/api/v1/**', (route) => route.fulfill({ status: 501, json: { error: 'Unexpected API request' } }));
  await page.route(`**${api}/auth/providers`, (route) => route.fulfill({ json: { enabled: false, providers: [] } }));
  await page.route(`**${api}/health`, (route) => route.fulfill({ body: 'ok' }));
  await page.route(`**${api}/graph_drafts?limit=50`, (route) => route.fulfill({ json: { items: [inventory, catalog, credits], next_cursor: null } }));
  await page.route(`**${api}/graphs/${inventory.id}/draft`, (route) => route.fulfill({ json: inventory }));
  await page.route(/\/api\/v1\/graphs\?limit=(50|200)$/, (route) => route.fulfill({ json: { items: versions, next_cursor: null } }));
  await page.route(`**${api}/graphs/${inventory.id}`, (route) => route.fulfill({ json: published }));
  await page.route(`**${api}/runs?graph_id=${inventory.id}&limit=50`, (route) => route.fulfill({ json: { items: [run], next_cursor: null } }));
  await page.route(`**${api}/runs/${run.uuid}`, (route) => route.fulfill({ json: runView }));
  await page.route(`**${api}/jobs?run_id=${run.uuid}&limit=50`, (route) => route.fulfill({ json: { items: [job], next_cursor: null } }));
  await page.route(`**${api}/agents?limit=50`, (route) => route.fulfill({ json: { items: [agent, { ...agent, id: 2, uuid: 'a3000002-0000-4000-8000-000000000000', online: false, active_jobs: 0 }], next_cursor: null } }));
}

async function mockSignIn(page: Page) {
  await mockWorkspace(page);
  await page.route(`**${api}/auth/providers`, (route) => route.fulfill({ json: {
    enabled: true, company_name: 'Rayflow', providers: [
      { id: 'local', label: 'Local account', kind: 'password' }, { id: 'oidc', label: 'Company SSO', kind: 'redirect' },
    ],
  } }));
  await page.route(`**${api}/auth/me`, (route) => route.fulfill({ status: 401, json: { error: 'Sign in required' } }));
}

test('Schedule and Agents update on manual refresh and health is checked once', async ({ page }) => {
  await mockWorkspace(page);
  let jobStatus: Job['status'] = 'Polled';
  let agentOnline = true;
  const requests = { health: 0, jobs: 0, agents: 0 };
  await page.route(`**${api}/health`, (route) => {
    requests.health += 1;
    return route.fulfill({ body: 'ok' });
  });
  await page.route((url) => url.pathname === `${api}/jobs` && url.searchParams.get('limit') === '50', (route) => {
    requests.jobs += 1;
    return route.fulfill({ json: { items: [{ ...job, status: jobStatus }], next_cursor: null } });
  });
  await page.route(`**${api}/agents?limit=50`, (route) => {
    requests.agents += 1;
    return route.fulfill({ json: { items: [{ ...agent, online: agentOnline }], next_cursor: null } });
  });
  await page.clock.install();
  await page.goto('/#/schedule');
  const table = page.getByRole('table');
  const refresh = page.getByRole('button', { name: 'Refresh', exact: true });
  await expect(page.getByTitle('API connected', { exact: true })).toBeVisible();
  await expect(table.getByText('Polled', { exact: true })).toBeVisible();
  jobStatus = 'Succeeded';
  await page.clock.fastForward(20_000);
  await expect(table.getByText('Polled', { exact: true })).toBeVisible();
  expect(requests).toEqual({ health: 1, jobs: 1, agents: 0 });
  await refresh.click();
  await expect(table.getByText('Succeeded', { exact: true })).toBeVisible();

  await page.getByRole('navigation').getByRole('link', { name: /^Agents/ }).click();
  await expect(table.getByText('Online', { exact: true })).toBeVisible();
  agentOnline = false;
  await page.clock.fastForward(20_000);
  await expect(table.getByText('Online', { exact: true })).toBeVisible();
  expect(requests).toEqual({ health: 1, jobs: 2, agents: 1 });
  await refresh.click();
  await expect(table.getByText('Offline', { exact: true })).toBeVisible();
  expect(requests).toEqual({ health: 1, jobs: 2, agents: 2 });
});

async function checkLayout(page: Page, width: number, view: string, theme: ColorScheme) {
  expect(await page.evaluate(() => document.documentElement.scrollWidth), `${view} at ${width}px`).toBeLessThanOrEqual(width);
  if (view === 'drafts' || view === 'published') {
    const times = page.locator('time');
    await expect(times).toHaveCount(6);
    const fits = await times.evaluateAll((elements) => elements.every((element) => {
      const range = document.createRange();
      range.selectNodeContents(element);
      const [line, extra] = Array.from(range.getClientRects());
      const cell = element.closest('dd, td')!.getBoundingClientRect();
      return Boolean(line && !extra && line.left >= cell.left - 1 && line.right <= cell.right + 1);
    }));
    expect(fits, `Audit timestamps fit on one line in ${view} at ${width}px`).toBe(true);
  }
  if (process.env.RAYFLOW_CAPTURE_VISUALS && width !== 768 && (width !== 320 || ['runtime', 'schedule'].includes(view))) {
    const filename = theme === 'dark' ? `dark-${view}-${width}` : width === 320 ? `${view}-320` : view;
    await page.screenshot({ path: test.info().outputPath(`${filename}.png`), fullPage: !['input-files', 'output-files'].includes(view), animations: 'disabled' });
  }
}

async function buttonColors(button: Locator) {
  return button.evaluate((element) => {
    const luminance = (color: string) => {
      const rgb = color.match(/[\d.]+/g)!.slice(0, 3).map((value) => Number(value) / 255);
      const [r, g, b] = rgb.map((value) => value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4);
      return 0.2126 * r + 0.7152 * g + 0.0722 * b;
    };
    const ratio = (a: number, b: number) => (Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05);
    let backdrop = 'rgb(255, 255, 255)';
    for (let parent = element.parentElement; parent; parent = parent.parentElement) {
      const color = getComputedStyle(parent).backgroundColor;
      if (color !== 'rgba(0, 0, 0, 0)') { backdrop = color; break; }
    }
    const style = getComputedStyle(element);
    const fill = style.backgroundColor === 'rgba(0, 0, 0, 0)' ? backdrop : style.backgroundColor;
    return { textContrast: ratio(luminance(style.color), luminance(fill)), fillContrast: ratio(luminance(fill), luminance(backdrop)) };
  });
}

async function expectStrongerHover(button: Locator) {
  const idle = (await buttonColors(button)).fillContrast;
  expect(idle).toBeGreaterThan(1);
  await button.hover();
  await expect.poll(async () => (await buttonColors(button)).fillContrast).toBeGreaterThan(idle);
}

const touchTest = test.extend({ hasTouch: true });
for (const { theme, width } of themes.flatMap((theme) => [320, 390, 768, 1440].map((width) => ({ theme, width })))) {
  const viewportTest = width <= 390 ? touchTest : test;
  viewportTest(`Console views stay within the ${width}px viewport${theme === 'dark' ? ' in dark mode' : ''}`, async ({ page }) => {
    await page.setViewportSize({ width, height: width === 1440 ? 1000 : 844 });
    await page.emulateMedia({ colorScheme: theme });
    await mockWorkspace(page);
    await page.goto('/#/orchestration');
    await expect(page.locator('html')).toHaveAttribute('data-theme', theme);

    const drafts = page.getByRole('table', { name: 'Draft workflows' });
    const draftRows = drafts.getByRole('row').filter({ has: page.getByRole('cell') });
    const draft = draftRows.filter({ has: page.getByRole('button', { name: inventory.name, exact: true }) });
    await expect(draftRows).toHaveCount(3);
    const create = page.getByRole('button', { name: 'New workflow', exact: true });
    if (width <= 390) {
      expect(await create.evaluate((element) => element.getBoundingClientRect().height)).toBeGreaterThanOrEqual(44);
    }
    if (width === 1440) {
      await expectStrongerHover(page.getByRole('button', { name: 'Refresh', exact: true }));
      await expectStrongerHover(draft.getByRole('button', { name: 'Edit', exact: true }));
      await page.mouse.move(0, 0);
    }
    await checkLayout(page, width, 'drafts', theme);
    await create.click();
    const dialog = page.getByRole('dialog', { name: 'Create a workflow' });
    const name = dialog.getByLabel('Workflow name');
    await name.click();
    await expect(name).toBeFocused();
    await checkLayout(page, width, 'create-workflow', theme);
    await dialog.getByRole('button', { name: 'Cancel', exact: true }).click();
    await draft.getByRole('button', { name: 'Edit', exact: true }).click();
    await expect(page.getByRole('textbox', { name: 'Workflow name' })).toHaveValue(inventory.name);
    await expect(page.getByTestId('flow-canvas').getByText(inventory.nodes[0].name, { exact: true })).toBeVisible();
    await checkLayout(page, width, 'editor', theme);
    await page.getByRole('button', { name: 'Back to workflows' }).click();

    await page.getByRole('button', { name: 'Published versions', exact: true }).click();
    const table = page.getByRole('table', { name: 'Published versions' });
    await expect(table.getByRole('row')).toHaveCount(4);
    const currentVersion = table.getByRole('row').filter({ hasText: inventory.name })
      .filter({ has: page.getByRole('cell', { name: 'v3', exact: true }) });
    if (width === 1440) {
      await expectStrongerHover(currentVersion.getByRole('button', { name: 'Edit', exact: true }));
      await page.mouse.move(0, 0);
    }
    await checkLayout(page, width, 'published', theme);
    await currentVersion.getByRole('button', { name: 'Runtime', exact: true }).click();

    const inspectRun = page.getByRole('button', { name: `Inspect run ${run.uuid}`, exact: true });
    await expect(inspectRun).toBeVisible();
    if (width <= 390) {
      const startRun = page.getByRole('button', { name: 'Start a run', exact: true });
      const picker = page.locator('.workflow-picker-control');
      await expect(picker.getByRole('combobox', { name: 'Search workflows' })).toBeVisible();
      for (const control of [picker, startRun]) {
        expect(await control.evaluate((element) => element.getBoundingClientRect().height)).toBeGreaterThanOrEqual(44);
      }
    }
    await checkLayout(page, width, 'runtime', theme);
    await inspectRun.click();
    const canvas = page.getByTestId('flow-canvas');
    await canvas.getByText(inventory.nodes[1].name, { exact: true }).click();
    await expect(page.getByTitle(run.data_id, { exact: true })).toHaveText(run.data_id.slice(0, 6));
    const nodeId = inventory.nodes[1].id;
    await expect(page.getByRole('complementary').getByTitle(nodeId, { exact: true })).toHaveText(nodeId.slice(0, 6));
    await checkLayout(page, width, 'run-detail', theme);
    await page.getByRole('button', { name: 'View input files', exact: true }).click();
    const inputDrawer = page.getByRole('dialog', { name: 'Input files', exact: true });
    await expect(inputDrawer.getByText(inputFiles[0].file_name, { exact: true })).toBeVisible();
    await expectDrawerAlignedWithInspector(inputDrawer);
    await checkLayout(page, width, 'input-files', theme);
    await page.keyboard.press('Escape');
    await expect(inputDrawer).toBeHidden();
    await canvas.getByText(inventory.nodes[0].name, { exact: true }).click();
    await page.getByRole('button', { name: 'View output files', exact: true }).click();
    const outputDrawer = page.getByRole('dialog', { name: 'Output files', exact: true });
    await expect(outputDrawer.getByRole('button', { name: `Download output ${outputFiles[0].file_name}`, exact: true })).toBeVisible();
    await expectDrawerAlignedWithInspector(outputDrawer);
    await checkLayout(page, width, 'output-files', theme);
    await page.keyboard.press('Escape');
    await expect(outputDrawer).toBeHidden();

    await page.getByRole('button', { name: 'View jobs', exact: true }).click();
    await expect(page.getByRole('table').getByTitle(run.uuid, { exact: true })).toHaveText(`Run ${run.uuid.slice(0, 6)}`);
    await checkLayout(page, width, 'schedule', theme);

    const menu = page.getByRole('button', { name: 'Open navigation', exact: true });
    if (await menu.isVisible()) await menu.click();
    await page.getByRole('navigation').getByRole('link', { name: /^Agents/ }).click();
    await expect(page.getByRole('table').getByText('Online', { exact: true })).toBeVisible();
    await checkLayout(page, width, 'agents', theme);
  });
}

for (const theme of themes) {
  test(`Loading respects reduced motion and the error state can be retried by keyboard${theme === 'dark' ? ' in dark mode' : ''}`, async ({ page }) => {
    const width = 390;
    await page.setViewportSize({ width, height: 844 });
    await page.emulateMedia({ reducedMotion: 'reduce', colorScheme: theme });
    await mockWorkspace(page);
    let release!: () => void;
    const pending = new Promise<void>((resolve) => { release = resolve; });
    let shouldFail = true;
    await page.route(`**${api}/graph_drafts?limit=50`, async (route) => {
      if (shouldFail) {
        await pending;
        await route.fulfill({ status: 500, json: { error: 'Unable to load workflows' } });
      } else {
        await route.fulfill({ json: { items: [], next_cursor: null } });
      }
    });
    try {
      await page.goto('/#/orchestration');
      await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
      const loading = page.getByRole('status', { name: 'Loading workspace…' });
      await expect(loading).toContainText('Loading workspace…');
      const skeleton = loading.locator('.skeleton[aria-hidden=true]');
      await expect(skeleton).toBeVisible();
      const animates = await skeleton.evaluate((element) => element.getAnimations({ subtree: true })
        .some((animation) => Number(animation.effect?.getComputedTiming().activeDuration ?? 0) > 10));
      expect(animates).toBe(false);
      await checkLayout(page, width, 'loading', theme);
    } finally {
      release();
    }

    const error = page.getByRole('alert');
    await expect(error).toContainText('Unable to load workflows');
    const retry = error.getByRole('button', { name: 'Try again' });
    await retry.focus();
    await page.keyboard.press('Shift+Tab');
    await page.keyboard.press('Tab');
    await expect(retry).toBeFocused();
    const focusVisible = await retry.evaluate((element) => {
      const style = getComputedStyle(element);
      return element.matches(':focus-visible') && style.outlineStyle !== 'none' && parseFloat(style.outlineWidth) >= 1;
    });
    expect(focusVisible).toBe(true);
    await checkLayout(page, width, 'error', theme);
    shouldFail = false;
    await retry.press('Enter');
    await expect(error).toHaveCount(0);
    await expect(page.getByRole('heading', { name: 'Your next workflow starts here' })).toBeVisible();
    const create = page.getByRole('button', { name: 'New workflow', exact: true });
    expect((await buttonColors(create)).textContrast).toBeGreaterThanOrEqual(4.5);
    await create.hover();
    expect((await buttonColors(create)).textContrast).toBeGreaterThanOrEqual(4.5);
    await page.mouse.down();
    try {
      expect(await create.evaluate((element) => element.matches(':active'))).toBe(true);
      expect((await buttonColors(create)).textContrast).toBeGreaterThanOrEqual(4.5);
    } finally {
      await page.mouse.move(0, 0);
      await page.mouse.up();
    }
    await checkLayout(page, width, 'empty', theme);
  });
}

for (const { theme, width } of themes.flatMap((theme) => [390, 1440].map((width) => ({ theme, width })))) {
  test(`Sign-in keeps both providers and its keyboard controls reachable at ${width}px${theme === 'dark' ? ' in dark mode' : ''}`, async ({ page }) => {
    await page.setViewportSize({ width, height: width === 1440 ? 1000 : 844 });
    await page.emulateMedia({ colorScheme: theme });
    await mockSignIn(page);
    await page.goto('/');
    await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
    await expect(page.getByRole('heading', { name: 'Welcome to Rayflow' })).toBeVisible();
    await expect(page.getByRole('combobox', { name: 'Theme', exact: true })).toHaveValue('system');
    await expect(page.getByRole('link', { name: 'Continue with company SSO' })).toBeVisible();
    await expect(page.getByLabel('Username')).toBeFocused();
    await page.keyboard.press('Tab');
    await expect(page.getByLabel('Password')).toBeFocused();
    await page.keyboard.press('Tab');
    await expect(page.getByRole('button', { name: 'Sign in', exact: true })).toBeFocused();
    await checkLayout(page, width, 'sign-in', theme);
  });
}

test('System follows operating system changes and explicit themes persist across reloads', async ({ page }) => {
  await page.emulateMedia({ colorScheme: 'dark' });
  await mockWorkspace(page);
  await page.goto('/#/orchestration');
  const root = page.locator('html');
  const theme = page.getByRole('combobox', { name: 'Theme', exact: true });
  await expect(theme).toHaveValue('system');
  await expect(root).toHaveAttribute('data-theme', 'dark');
  await page.emulateMedia({ colorScheme: 'light' });
  await expect(root).toHaveAttribute('data-theme', 'light');
  await page.emulateMedia({ colorScheme: 'dark' });
  await expect(root).toHaveAttribute('data-theme', 'dark');

  await theme.selectOption('light');
  await expect(root).toHaveAttribute('data-theme', 'light');
  await page.reload();
  await expect(theme).toHaveValue('light');
  await expect(root).toHaveCSS('color-scheme', 'light');
  expect(await page.evaluate(() => localStorage.getItem('rayflow.theme'))).toBe('light');
  await theme.selectOption('dark');
  await page.emulateMedia({ colorScheme: 'light' });
  await page.reload();
  await expect(theme).toHaveValue('dark');
  await expect(root).toHaveAttribute('data-theme', 'dark');
  expect(await page.evaluate(() => localStorage.getItem('rayflow.theme'))).toBe('dark');
  await theme.selectOption('system');
  await expect(root).toHaveAttribute('data-theme', 'light');
  await page.emulateMedia({ colorScheme: 'dark' });
  await expect(root).toHaveAttribute('data-theme', 'dark');
});

test('Changing theme preserves a dirty workflow and makes no API requests', async ({ page }) => {
  await page.emulateMedia({ colorScheme: 'light' });
  await mockWorkspace(page);
  const requests: string[] = [];
  page.on('request', (request) => {
    const { pathname } = new URL(request.url());
    if (pathname.startsWith(api)) requests.push(`${request.method()} ${pathname}`);
  });
  await page.goto('/#/orchestration');
  await page.getByRole('table', { name: 'Draft workflows' }).getByRole('row')
    .filter({ has: page.getByRole('button', { name: inventory.name, exact: true }) })
    .getByRole('button', { name: 'Edit', exact: true }).click();
  const name = page.getByRole('textbox', { name: 'Workflow name' });
  await name.fill('Inventory edits still in progress');
  await expect(page.getByText('Unsaved changes', { exact: true })).toBeVisible();
  const beforeSwitch = requests.length;
  const theme = page.getByRole('combobox', { name: 'Theme', exact: true });
  for (const value of ['dark', 'light']) {
    await theme.selectOption(value);
    await expect(page.locator('html')).toHaveAttribute('data-theme', value);
    await expect(name).toHaveValue('Inventory edits still in progress');
    await expect(page.getByText('Unsaved changes', { exact: true })).toBeVisible();
  }
  expect(requests.slice(beforeSwitch)).toEqual([]);
});

test('An invalid saved theme falls back to System', async ({ page }) => {
  await page.emulateMedia({ colorScheme: 'dark' });
  await page.addInitScript(() => localStorage.setItem('rayflow.theme', 'invalid'));
  await mockWorkspace(page);
  await page.goto('/#/orchestration');
  await expect(page.getByRole('combobox', { name: 'Theme', exact: true })).toHaveValue('system');
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
});

test('Sign-in can switch themes when local storage is unavailable', async ({ page }) => {
  await page.emulateMedia({ colorScheme: 'dark' });
  await page.addInitScript(() => Object.defineProperty(window, 'localStorage', {
    get() { throw new DOMException('Storage blocked', 'SecurityError'); },
  }));
  await mockSignIn(page);
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Welcome to Rayflow' })).toBeVisible();
  const theme = page.getByRole('combobox', { name: 'Theme', exact: true });
  await expect(theme).toHaveValue('system');
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
  await theme.selectOption('light');
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');
});
