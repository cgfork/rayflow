import { useState, type FormEvent } from 'react';
import { Check, Pencil, Plus, X } from 'lucide-react';
import { ErrorBanner, Modal } from './components';
import { DockerConfigInput, parseDockerConfig } from './DockerConfigInput';
import { bytesPerMiB, isOptionalGpuCdiDevice, isOptionalGpuMemory, isOverrideNameValid } from './graph';
import type { AnnotationMatchNodeSettings, NodeSettings } from './types';

type Overrides = NonNullable<NodeSettings['overrides']>;

export default function NodeOverrides({ settings, allowGpuMemory, update }: { settings: NodeSettings; allowGpuMemory: boolean; update: (overrides: Overrides) => void }) {
  const [editor, setEditor] = useState<{ name?: string }>();
  const overrides = settings.overrides ?? {};

  function remove(name: string) {
    const next = { ...overrides };
    delete next[name];
    update(next);
  }

  return <div className="field-section">
    <h3>Annotation overrides <button type="button" className="button small" onClick={() => setEditor({})}><Plus size={14} />Add override</button></h3>
    {Object.entries(overrides).map(([name, override]) => <div key={name} className="node-override">
      <div>
        <strong>{name || '(unnamed)'}</strong>
        {override.annotation_match && <code title={override.annotation_match}>{override.annotation_match}</code>}
      </div>
      <div className="actions">
        <button type="button" className="icon-button" aria-label={`Edit override ${name}`} onClick={() => setEditor({ name })}><Pencil size={14} /></button>
        <button type="button" className="icon-button" aria-label={`Remove override ${name}`} onClick={() => remove(name)}><X size={14} /></button>
      </div>
    </div>)}
    {editor && <OverrideEditor
      originalName={editor.name}
      overrides={overrides}
      baseResource={settings.resource}
      allowGpuMemory={allowGpuMemory}
      close={() => setEditor(undefined)}
      apply={(name, override) => {
        const next = { ...overrides, [name]: override };
        if (editor.name !== undefined && editor.name !== name) delete next[editor.name];
        update(next);
        setEditor(undefined);
      }}
    />}
  </div>;
}

function OverrideEditor({ originalName, overrides, baseResource, allowGpuMemory, close, apply }: {
  originalName?: string;
  overrides: Overrides;
  baseResource: NodeSettings['resource'];
  allowGpuMemory: boolean;
  close: () => void;
  apply: (name: string, override: AnnotationMatchNodeSettings) => void;
}) {
  const current = originalName === undefined ? undefined : overrides[originalName];
  const resource = current?.resource ?? baseResource;
  const [name, setName] = useState(originalName ?? '');
  const [expression, setExpression] = useState(current?.annotation_match ?? '');
  const [timeout, setTimeoutText] = useState(current?.timeout ?? '');
  const initialConfig = current?.config == null ? '' : JSON.stringify(current.config, null, 2);
  const [configJson, setConfigJson] = useState(initialConfig);
  const [overrideResources, setOverrideResources] = useState(current?.resource != null);
  const [cpu, setCpu] = useState(String(resource.cpu_cores));
  const [memory, setMemory] = useState(String(resource.memory_bytes / bytesPerMiB));
  const [gpuMemory, setGpuMemory] = useState(resource.gpu_memory_bytes == null ? '' : String(resource.gpu_memory_bytes / bytesPerMiB));
  const [gpuDevice, setGpuDevice] = useState(resource.gpu_cdi_device ?? '');
  const [error, setError] = useState<string>();

  function submit(event: FormEvent) {
    event.preventDefault();
    try {
      if (!isOverrideNameValid(name)) throw new Error('Use a name up to 255 bytes with no trailing space. “default” is reserved for the base settings.');
      if (name !== originalName && Object.hasOwn(overrides, name)) throw new Error('An override with this name already exists.');
      const config = configJson.trim() ? parseDockerConfig(configJson) : undefined;
      const memoryBytes = Math.round(Number(memory) * bytesPerMiB);
      if (overrideResources && !Number.isSafeInteger(memoryBytes)) throw new Error('Memory exceeds the supported range.');
      const gpuMemoryBytes = gpuMemory === '' ? undefined : Math.round(Number(gpuMemory) * bytesPerMiB);
      if (overrideResources && !isOptionalGpuMemory(gpuMemoryBytes)) throw new Error('GPU memory must be positive whole bytes within the supported range.');
      const gpuCdiDevice = gpuDevice || undefined;
      if (overrideResources && !isOptionalGpuCdiDevice(gpuCdiDevice, gpuMemoryBytes)) throw new Error('Specify a qualified GPU CDI device name and positive GPU memory.');
      apply(name, {
        annotation_match: expression.trim() ? expression : undefined,
        config,
        resource: overrideResources ? { cpu_cores: Number(cpu), memory_bytes: memoryBytes, gpu_memory_bytes: gpuMemoryBytes, gpu_cdi_device: gpuCdiDevice } : undefined,
        timeout: timeout.trim() || undefined,
      });
    } catch (failure) { setError((failure as Error).message); }
  }

  return <Modal title={originalName === undefined ? 'Add annotation override' : 'Edit annotation override'} close={close}>
    <form onSubmit={submit}>
      <ErrorBanner error={error} />
      <label className="field">Override name<input autoFocus required={originalName !== ''} value={name} onChange={(event) => setName(event.target.value)} /></label>
      <label className="field">Annotation match (CEL)
        <textarea className="mono" rows={2} value={expression} placeholder={'has(annotations.priority) && annotations.priority == "high"'} spellCheck={false} onChange={(event) => setExpression(event.target.value)} />
      </label>
      <label className="field">Timeout<input value={timeout} placeholder="Use base timeout" onChange={(event) => setTimeoutText(event.target.value)} /></label>
      <label className="checkbox"><input type="checkbox" checked={overrideResources} onChange={(event) => setOverrideResources(event.target.checked)} />Override resources</label>
      {overrideResources && <div className="settings-resources">
        <label className="field">CPU cores<input type="number" min={1} max={4294967295} step={1} required value={cpu} onChange={(event) => setCpu(event.target.value)} /></label>
        <label className="field">Memory (MiB)<input type="number" min={1 / bytesPerMiB} step="any" required value={memory} onChange={(event) => setMemory(event.target.value)} /></label>
      </div>}
      {overrideResources && allowGpuMemory && <label className="field">GPU memory (MiB)<input
        type="number" min={1 / bytesPerMiB} step="any" placeholder="Optional"
        value={gpuMemory} onChange={(event) => setGpuMemory(event.target.value)}
      /></label>}
      {overrideResources && allowGpuMemory && <label className="field">GPU CDI device<input
        className="mono" placeholder="Automatic" maxLength={255}
        value={gpuDevice} onChange={(event) => setGpuDevice(event.target.value)}
      /></label>}
      <div className="field">
        <span>Docker configuration JSON</span>
        <DockerConfigInput initialValue={initialConfig} onChange={setConfigJson} descriptionId="override-config-help" allowBlank />
      </div>
      <p className="field-hint" id="override-config-help">Leave blank to use the base configuration. A JSON object replaces it.</p>
      <div className="modal-actions"><button className="button" type="button" onClick={close}>Cancel</button><button className="button primary" type="submit"><Check size={15} />Apply override</button></div>
    </form>
  </Modal>;
}
