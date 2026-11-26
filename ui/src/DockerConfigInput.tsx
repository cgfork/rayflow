import { lazy, Suspense, type ComponentProps } from 'react';
import { isJsonObject } from './graph';

const JsonCodeEditor = lazy(() => import('./JsonCodeEditor'));

export function DockerConfigInput(props: Omit<ComponentProps<typeof JsonCodeEditor>, 'label'>) {
  return <Suspense fallback={<div className="json-code-editor" role="status">Loading editor…</div>}>
    <JsonCodeEditor {...props} label="Docker configuration JSON" />
  </Suspense>;
}

export function parseDockerConfig(text: string) {
  const value: unknown = JSON.parse(text);
  if (!isJsonObject(value)) throw new Error('Docker configuration must be a JSON object.');
  return value;
}
