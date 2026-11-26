import { lazy, Suspense, type ComponentProps } from 'react';
import { Plus, X } from 'lucide-react';
import { isJsonObject, isStringArray } from './graph';
import type { NodeSettings } from './types';

const JsonCodeEditor = lazy(() => import('./JsonCodeEditor'));

type DockerConfig = NodeSettings['config'];

export function DockerConfigFields({ config, change }: { config: DockerConfig; change: (config: DockerConfig) => void }) {
  function changeList(field: 'command' | 'env', items: string[]) {
    const next = { ...config };
    if (items.length) next[field] = items;
    else delete next[field];
    change(next);
  }

  return <>
    <label className="field">Docker image
      <input placeholder="alpine:3.21" value={typeof config?.image === 'string' ? config.image : ''} onChange={(event) => change({ ...config, image: event.target.value })} />
    </label>
    <DockerConfigList title="Command" itemLabel="Command argument" addLabel="Add argument" placeholder="Argument"
      value={config?.command} change={(items) => changeList('command', items)} />
    <DockerConfigList title="Environment variables" itemLabel="Environment variable" addLabel="Add variable" placeholder="NAME=value"
      value={config?.env} change={(items) => changeList('env', items)} />
  </>;
}

function DockerConfigList({ title, itemLabel, addLabel, placeholder, value, change }: {
  title: string;
  itemLabel: string;
  addLabel: string;
  placeholder: string;
  value: unknown;
  change: (items: string[]) => void;
}) {
  const items = value === undefined ? [] : isStringArray(value) ? value : undefined;
  return <div className="docker-config-list" role="group" aria-label={title}>
    <div className="docker-config-list-heading">
      <h4>{title}</h4>
      {items && <button type="button" className="button small" onClick={() => change([...items, ''])}><Plus size={14} />{addLabel}</button>}
    </div>
    {items ? items.map((item, index) => <div className="docker-config-row actions" key={index}>
      <textarea rows={1} spellCheck={false} aria-label={`${itemLabel} ${index + 1}`} placeholder={placeholder} value={item}
        onChange={(event) => change(items.map((existing, offset) => offset === index ? event.target.value : existing))} />
      <button type="button" className="icon-button" aria-label={`Remove ${itemLabel.toLowerCase()} ${index + 1}`}
        onClick={() => change(items.filter((_, offset) => offset !== index))}><X size={14} /></button>
    </div>) : <p className="field-hint">Use the JSON editor to set an array of strings.</p>}
  </div>;
}

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
