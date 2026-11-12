import { expect, test, type Page } from '@playwright/test';

async function backend(page: Page) {
  let signedIn = false;
  let expired = false;
  const user = { id: 'stable-user-id', name: 'Alice Example', provider: 'local', subject: 'alice' };
  await page.route('https://fonts.googleapis.com/**', (route) => route.abort());
  await page.route('**/api/**', async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname.replace('/api', '');
    if (path === '/auth/providers') return route.fulfill({ json: { enabled: true, company_name: 'Example Company', providers: [
      { id: 'local', label: 'Rayflow account', kind: 'password' }, { id: 'ldap', label: 'Company directory', kind: 'password' },
      { id: 'oidc', label: 'Company SSO', kind: 'redirect' },
    ] } });
    if (path === '/auth/me') return route.fulfill({ status: signedIn ? 200 : 401, json: signedIn ? user : { error: 'Sign in to continue' } });
    if (path === '/auth/login') {
      expect(request.headers()['x-rayflow-request']).toBe('1');
      const body = request.postDataJSON();
      signedIn = body.username === 'alice' && body.password === 'correct password';
      return route.fulfill({ status: signedIn ? 200 : 401, json: signedIn ? user : { error: 'Invalid login credentials or expired sign-in request' } });
    }
    if (path === '/auth/logout') {
      expect(request.headers()['x-rayflow-request']).toBe('1');
      signedIn = false; return route.fulfill({ status: 204 });
    }
    if (path === '/health') return route.fulfill({ body: 'ok' });
    if (!signedIn || expired) return route.fulfill({ status: 401, json: { error: 'Sign in to continue' } });
    return route.fulfill({ json: { items: [], next_cursor: null } });
  });
  return { expire: () => { expired = true; } };
}

test('signs in, restores the session, and signs out without editable audit identity', async ({ page }) => {
  await backend(page);
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Welcome to Example Company' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Continue with company SSO' })).toHaveAttribute('href', '/api/auth/oidc/login');
  await page.getByLabel('Account type').selectOption('ldap');
  await page.getByLabel('Username').fill('alice');
  await page.getByLabel('Password', { exact: true }).fill('wrong');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('Invalid login');
  await expect(page.getByLabel('Password', { exact: true })).toHaveValue('');
  await page.getByLabel('Password', { exact: true }).fill('correct password');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByText('Alice Example', { exact: true })).toBeVisible();
  await expect(page.getByLabel('Audit identity')).toHaveCount(0);
  await page.reload();
  await expect(page.getByText('Alice Example', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Sign out', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Sign in', exact: true })).toBeVisible();
  expect(await page.evaluate(() => Object.keys(localStorage).filter((key) => /token|password|session/.test(key)))).toEqual([]);
});

test('expired sessions return to login and company login failures remain recoverable', async ({ page }) => {
  const control = await backend(page);
  await page.goto('/?login_error=1');
  await expect(page.getByRole('alert')).toContainText('Company sign-in failed');
  await page.getByLabel('Username').fill('alice');
  await page.getByLabel('Password', { exact: true }).fill('correct password');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByText('Alice Example', { exact: true })).toBeVisible();
  control.expire();
  await page.getByRole('link', { name: 'Agents Connected fleet' }).click();
  await expect(page.getByRole('alert')).toContainText('Your session has expired');
  await expect(page.getByRole('button', { name: 'Sign in', exact: true })).toBeVisible();
});

test('login works on narrow screens and configuration failures do not reveal the console', async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 812 });
  await page.route('**/api/auth/providers', (route) => route.fulfill({ status: 503, json: { error: 'Authentication unavailable' } }));
  await page.goto('/');
  await expect(page.getByRole('alert')).toContainText('Authentication unavailable');
  await expect(page.getByRole('button', { name: 'Retry connection' })).toBeVisible();
  await expect(page.getByRole('navigation')).toHaveCount(0);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});
