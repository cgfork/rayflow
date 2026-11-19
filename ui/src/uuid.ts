/** UUID v4, including HTTP development origins where randomUUID is unavailable. */
export function randomUUID(): string {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export async function copyUuid(id: string): Promise<boolean> {
  try {
    if (navigator.clipboard) {
      await navigator.clipboard.writeText(id);
      return true;
    }
  } catch {
    // Some browsers restrict access even when the Clipboard API is present.
  }

  // The console can also be served over HTTP on a remote host.
  const focused = document.activeElement;
  const field = document.createElement('textarea');
  field.value = id;
  field.readOnly = true;
  field.tabIndex = -1;
  field.className = 'workflow-clipboard-fallback';
  document.body.append(field);
  try {
    field.select();
    return document.execCommand('copy');
  } catch {
    return false;
  } finally {
    field.remove();
    if (focused instanceof HTMLElement) focused.focus({ preventScroll: true });
  }
}
