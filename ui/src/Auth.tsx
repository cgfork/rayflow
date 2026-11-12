import { useCallback, useEffect, useState, type FormEvent } from 'react';
import { ArrowRight, GitBranch, LogIn } from 'lucide-react';
import App from './App';
import { api, ApiError, send } from './api';

export type User = { id: string; provider: string; subject: string; name: string; email?: string };
type Provider = { id: string; label: string; kind: 'password' | 'redirect' };
type Configuration = { enabled: boolean; company_name: string; providers: Provider[] };

export default function Auth() {
  const [config, setConfig] = useState<Configuration>();
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(() => new URLSearchParams(window.location.search).has('login_error') ? 'Company sign-in failed or expired. Please try again.' : '');
  const [provider, setProvider] = useState('');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [busy, setBusy] = useState(false);
  const [retry, setRetry] = useState(0);

  useEffect(() => {
    const url = new URL(window.location.href);
    if (url.searchParams.has('login_error')) {
      url.searchParams.delete('login_error');
      history.replaceState(null, '', `${url.pathname}${url.search}${url.hash}`);
    }
  }, []);
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    async function load() {
      try {
        const configuration = await api<Configuration>('/auth/providers', { signal: controller.signal });
        let current: User | null = null;
        if (configuration.enabled) {
          try { current = await api<User>('/auth/me', { signal: controller.signal }); }
          catch (error) { if (!(error instanceof ApiError && error.status === 401)) throw error; }
        }
        if (!controller.signal.aborted) {
          setConfig(configuration); setUser(current);
          setProvider(configuration.providers.find((item) => item.kind === 'password')?.id || '');
        }
      } catch (error) {
        if (!controller.signal.aborted) { setConfig(undefined); setError(error instanceof Error ? error.message : 'Unable to load sign-in settings'); }
      } finally { if (!controller.signal.aborted) setLoading(false); }
    }
    void load();
    return () => controller.abort();
  }, [retry]);
  useEffect(() => {
    function expired() { setUser(null); setPassword(''); setError('Your session has expired. Please sign in again.'); }
    window.addEventListener('rayflow:unauthorized', expired);
    return () => window.removeEventListener('rayflow:unauthorized', expired);
  }, []);

  const logout = useCallback(async () => {
    await send<void>('/auth/logout', {});
    setUser(null); setPassword(''); setError('');
  }, []);
  async function login(event: FormEvent) {
    event.preventDefault(); setBusy(true); setError('');
    try { setUser(await send<User>('/auth/login', { provider, username, password })); }
    catch (error) { setError(error instanceof Error ? error.message : 'Sign-in failed'); }
    finally { setBusy(false); setPassword(''); }
  }

  if (!loading && config && (!config.enabled || user)) return <App user={user} logout={logout} />;
  const passwords = config?.providers.filter((item) => item.kind === 'password') || [];
  return <div className="login-page"><section className="login-card" aria-label="Sign in">
    <a className="brand" href="/"><span className="brand-mark"><GitBranch size={25} /></span><span>rayflow<span className="brand-period">.</span></span></a>
    <div className="login-heading"><span className="login-symbol"><LogIn size={24} /></span><h1>Welcome to {config?.company_name || 'Rayflow'}</h1><p>Sign in to your workflow workspace.</p></div>
    {loading ? <p role="status">Loading sign-in…</p> : <>
      {error && <p className="login-error" role="alert">{error}</p>}
      {!config ? <button className="button primary" onClick={() => { setError(''); setRetry((value) => value + 1); }}>Retry connection</button> : <>
        {config.providers.some((item) => item.kind === 'redirect') && <a className="button primary sso-button" href="/api/auth/oidc/login">Continue with company SSO<ArrowRight size={16} /></a>}
        {passwords.length > 0 && <form onSubmit={login}>
          {config.providers.some((item) => item.kind === 'redirect') && <div className="login-divider">or use your account</div>}
          {passwords.length > 1 && <label>Account type<select value={provider} onChange={(event) => setProvider(event.target.value)} disabled={busy}>{passwords.map((item) => <option key={item.id} value={item.id}>{item.label}</option>)}</select></label>}
          <label>Username<input autoComplete="username" autoFocus value={username} maxLength={256} required disabled={busy} onChange={(event) => setUsername(event.target.value)} /></label>
          <label>Password<input type="password" autoComplete="current-password" value={password} maxLength={4096} required disabled={busy} onChange={(event) => setPassword(event.target.value)} /></label>
          <button className="button primary" type="submit" disabled={busy}>{busy ? 'Signing in…' : 'Sign in'}<ArrowRight size={16} /></button>
        </form>}
      </>}
    </>}
  </section></div>;
}
