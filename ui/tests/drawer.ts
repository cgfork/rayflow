import { expect, type Locator } from '@playwright/test';

export async function expectDrawerAlignedWithInspector(drawer: Locator) {
  await expect(drawer).toBeVisible();
  await expect.poll(() => drawer.evaluate((element) => {
    const drawerBounds = element.getBoundingClientRect();
    const panel = document.querySelector('.runtime-canvas .inspector')!.getBoundingClientRect();
    return Math.max(
      Math.abs(drawerBounds.top - panel.top),
      Math.abs(drawerBounds.right - panel.right),
      Math.abs(drawerBounds.bottom - panel.bottom),
      Math.abs(drawerBounds.height - panel.height),
      -drawerBounds.left, -drawerBounds.top,
      drawerBounds.right - window.innerWidth, drawerBounds.bottom - window.innerHeight,
    );
  })).toBeLessThanOrEqual(1);
}
