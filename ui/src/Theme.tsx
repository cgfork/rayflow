import { createContext, useCallback, useContext, useLayoutEffect, useMemo, useState, type ReactNode } from 'react';
import { Monitor, Moon, Sun } from 'lucide-react';

type ColorScheme = 'light' | 'dark';
type ThemePreference = ColorScheme | 'system';

const storageKey = 'rayflow.theme';
const systemQuery = '(prefers-color-scheme: dark)';
const themeIcons = { system: Monitor, light: Sun, dark: Moon };

const ThemeContext = createContext<{
  preference: ThemePreference;
  resolvedTheme: ColorScheme;
  chooseTheme: (preference: ThemePreference) => void;
} | null>(null);

function parsePreference(value: string | null): ThemePreference {
  return value === 'light' || value === 'dark' ? value : 'system';
}

function readPreference(): ThemePreference {
  try {
    return parsePreference(window.localStorage.getItem(storageKey));
  } catch {
    return 'system';
  }
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [preference, setPreference] = useState(readPreference);
  const [systemTheme, setSystemTheme] = useState<ColorScheme>(() => window.matchMedia?.(systemQuery).matches ? 'dark' : 'light');
  const resolvedTheme = preference === 'system' ? systemTheme : preference;

  useLayoutEffect(() => {
    const media = window.matchMedia?.(systemQuery);
    if (!media) return;

    const update = () => setSystemTheme(media.matches ? 'dark' : 'light');
    media.addEventListener('change', update);
    update();
    return () => media.removeEventListener('change', update);
  }, []);

  useLayoutEffect(() => {
    const root = document.documentElement;
    root.dataset.theme = resolvedTheme;
    root.style.colorScheme = resolvedTheme;

    const themeColor = document.querySelector<HTMLMetaElement>('meta[name="theme-color"]');
    const color = themeColor?.dataset[resolvedTheme];
    if (color) themeColor.content = color;
  }, [resolvedTheme]);

  const chooseTheme = useCallback((next: ThemePreference) => {
    setPreference(next);
    try {
      window.localStorage.setItem(storageKey, next);
    } catch {
      // Keep the selection for this page when storage is unavailable.
    }
  }, []);

  const theme = useMemo(() => ({ preference, resolvedTheme, chooseTheme }), [preference, resolvedTheme, chooseTheme]);
  return <ThemeContext.Provider value={theme}>{children}</ThemeContext.Provider>;
}

export function useTheme() {
  const theme = useContext(ThemeContext);
  if (!theme) throw new Error('ThemeProvider is required');
  return theme;
}

export function ThemePicker() {
  const { preference, chooseTheme } = useTheme();
  const Icon = themeIcons[preference];
  return <label className="theme-picker">
    <Icon size={15} aria-hidden="true" />
    <select aria-label="Theme" value={preference} onChange={(event) => chooseTheme(parsePreference(event.target.value))}>
      <option value="system">System</option>
      <option value="light">Light</option>
      <option value="dark">Dark</option>
    </select>
  </label>;
}
