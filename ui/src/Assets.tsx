import { useState, type FormEvent } from 'react';
import { FileDown, Files, Pencil, Plus, Upload, X } from 'lucide-react';
import { sha256 } from '@noble/hashes/sha2.js';
import { bytesToHex } from '@noble/hashes/utils.js';
import { isHttpDownloadUrl, memory, send, usePaged, useRemote } from './api';
import { Empty, ErrorBanner, Loading, Modal, PageHeader, Pagination } from './components';
import { isAssetPath } from './graph';
import type { AssetRef, AssetVersion } from './types';

type Presigned = { url: string; operation: 'upload' | 'download'; headers: Record<string, string> };
type PreparedAsset =
  | { state: 'pending'; asset_id: string; upload_id: string; upload: Presigned }
  | (AssetVersion & { state: 'published' });

export default function Assets({ notify }: { notify: (message: string) => void }) {
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState<string>();
  const [downloading, setDownloading] = useState<string>();
  const versions = usePaged<AssetVersion>('/assets');
  const rows = versions.data?.items ?? [];

  async function download(asset: AssetVersion) {
    setDownloading(asset.asset_id);
    setError(undefined);
    try {
      const { presigned } = await send<{ presigned: Presigned }>(`/assets/${asset.asset_id}/download`, {});
      if (presigned.operation !== 'download' || !isHttpDownloadUrl(presigned.url)) throw new Error('The server returned an invalid download URL.');
      const link = document.createElement('a');
      link.href = presigned.url;
      link.target = '_blank';
      link.rel = 'noopener noreferrer';
      link.click();
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : 'Download failed.');
    } finally {
      setDownloading(undefined);
    }
  }

  return <div className="page">
    <PageHeader eyebrow={<><Files size={15} /> Library</>} title="Assets" description="Publish versioned files for workflow nodes.">
      <button className="button primary" onClick={() => setUploading(true)}><Upload size={15} />Upload asset</button>
    </PageHeader>
    <ErrorBanner error={error} />
    <ErrorBanner error={versions.error} retry={versions.refresh} />
    <section className="panel">
      {versions.loading && !versions.data ? <Loading label="Loading assets…" /> : versions.data && !rows.length ?
        <Empty title="No published assets">Upload a file to make it available in the workflow editor.</Empty> : rows.length > 0 && <div className="table-scroll">
          <table aria-label="Published assets"><thead><tr><th>Name</th><th>Version</th><th>Size</th><th>Format</th><th>Actions</th></tr></thead>
            <tbody>{rows.map((asset) => <tr key={asset.asset_id}>
              <td><strong>{asset.name}</strong></td><td>{asset.version}</td><td>{memory(asset.size)}</td><td>{asset.format}</td>
              <td><button className="icon-button" type="button" aria-label={`Download ${asset.name} v${asset.version}`} disabled={Boolean(downloading)} onClick={() => void download(asset)}><FileDown size={17} /></button></td>
            </tr>)}</tbody>
          </table>
        </div>}
      {(versions.page > 1 || versions.data?.next_cursor != null) && <AssetPagination versions={versions} />}
    </section>
    {uploading && <UploadAsset close={() => setUploading(false)} published={(asset) => {
      setUploading(false);
      versions.refresh();
      notify(`Published ${asset.name} v${asset.version}.`);
    }} />}
  </div>;
}

type AssetPage = ReturnType<typeof usePaged<AssetVersion>>;

function AssetPagination({ versions }: { versions: AssetPage }) {
  return <Pagination page={versions.page} count={versions.data?.items.length ?? 0} more={versions.data?.next_cursor != null}
    previous={versions.previous} next={versions.next} />;
}

function UploadAsset({ close, published }: { close: () => void; published: (asset: AssetVersion) => void }) {
  const [name, setName] = useState('');
  const [version, setVersion] = useState('1');
  const [file, setFile] = useState<File>();
  const [status, setStatus] = useState('');
  const [error, setError] = useState<string>();

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!file || status) return;
    const number = Number(version);
    if (!Number.isSafeInteger(number) || number < 1) {
      setError('Enter a positive whole version number.');
      return;
    }

    setError(undefined);
    setStatus('Checking file…');
    try {
      const hash = sha256.create();
      const stream = file.stream().getReader();
      try {
        for (;;) {
          const { value, done } = await stream.read();
          if (done) break;
          hash.update(value);
        }
      } finally {
        stream.releaseLock();
      }
      const result = await send<PreparedAsset>('/assets/prepare', {
        name, version: number, size: file.size, format: file.type || 'application/octet-stream',
        sha256: bytesToHex(hash.digest()),
      });
      if (result.state === 'published') {
        published(result);
        return;
      }
      const { upload } = result;
      if (upload.operation !== 'upload' || !isHttpDownloadUrl(upload.url)) throw new Error('The server returned an invalid upload URL.');
      setStatus('Uploading to storage…');
      const response = await fetch(upload.url, {
        method: 'PUT', body: file, headers: upload.headers, credentials: 'omit', redirect: 'error',
      });
      if (!response.ok) throw new Error(`Object storage rejected the upload (HTTP ${response.status}).`);
      setStatus('Verifying and publishing…');
      const asset = await send<AssetVersion>(`/assets/${result.asset_id}/commit`, { upload_id: result.upload_id });
      published(asset);
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : 'Asset upload failed.');
    } finally {
      setStatus('');
    }
  }

  return <Modal title="Upload asset" close={() => { if (!status) close(); }}>
    <form onSubmit={(event) => void submit(event)}>
      <ErrorBanner error={error} />
      <label className="field">Asset name<input required maxLength={255} pattern="[A-Za-z0-9][A-Za-z0-9._\\-]*" value={name} onChange={(event) => setName(event.target.value)} disabled={Boolean(status)} /></label>
      <label className="field">Version<input required type="number" min="1" step="1" value={version} onChange={(event) => setVersion(event.target.value)} disabled={Boolean(status)} /></label>
      <label className="field">File<input required type="file" onChange={(event) => setFile(event.target.files?.[0])} disabled={Boolean(status)} /></label>
      {status && <p role="status" className="muted">{status}</p>}
      <div className="modal-actions">
        <button type="button" className="button" onClick={close} disabled={Boolean(status)}>Cancel</button>
        <button type="submit" className="button primary" disabled={Boolean(status) || !file}><Upload size={15} />Publish asset</button>
      </div>
    </form>
  </Modal>;
}

export function NodeAssets({ assets, change }: { assets: AssetRef[]; change: (assets: AssetRef[]) => void }) {
  const [editing, setEditing] = useState<number>();
  return <div className="field-section">
    <h3>Assets <button type="button" className="icon-button" aria-label="Add asset" onClick={() => setEditing(assets.length)}><Plus size={15} /></button></h3>
    {assets.map((asset, index) => <div className="port-form" key={`${asset.asset_id}/${asset.file_name}/${index}`}>
      <div className="actions">
        <div className="asset-reference"><strong>{asset.file_name}</strong><AssetLabel assetId={asset.asset_id} /></div>
        <button type="button" className="icon-button" aria-label={`Edit asset ${index + 1}`} onClick={() => setEditing(index)}><Pencil size={14} /></button>
        <button type="button" className="icon-button" aria-label={`Remove asset ${index + 1}`} onClick={() => change(assets.filter((_, position) => position !== index))}><X size={14} /></button>
      </div>
    </div>)}
    {editing !== undefined && <AssetPicker initial={assets[editing]} existing={assets.filter((_, index) => index !== editing)}
      close={() => setEditing(undefined)} save={(asset) => {
        const next = [...assets];
        next[editing] = asset;
        change(next);
        setEditing(undefined);
      }} />}
  </div>;
}

function AssetLabel({ assetId }: { assetId: string }) {
  const { data } = useRemote<AssetVersion>(`/assets/${encodeURIComponent(assetId)}`);
  return <small className="muted" title={assetId}>{data ? `${data.name} · v${data.version}` : assetId}</small>;
}

function AssetPicker({ initial, existing, save, close }: {
  initial?: AssetRef;
  existing: AssetRef[];
  save: (asset: AssetRef) => void;
  close: () => void;
}) {
  const [selected, setSelected] = useState(initial?.asset_id ?? '');
  const [fileName, setFileName] = useState(initial?.file_name ?? '');
  const versions = usePaged<AssetVersion>('/assets');
  const current = versions.data?.items ?? [];
  const overlap = existing.some(({ file_name }) =>
    file_name === fileName || file_name.startsWith(`${fileName}/`) || fileName.startsWith(`${file_name}/`),
  );

  return <Modal title={initial ? 'Edit asset' : 'Add asset'} close={close}>
    <form onSubmit={(event) => {
      event.preventDefault();
      if (selected && isAssetPath(fileName) && !overlap) save({ file_name: fileName, asset_id: selected });
    }}>
      <ErrorBanner error={versions.error} retry={versions.refresh} />
      <label className="field">Asset version<select required value={selected} onChange={(event) => setSelected(event.target.value)}>
        <option value="">Select a version</option>
        {selected && !current.some(({ asset_id }) => asset_id === selected) && <option value={selected}>{selected}</option>}
        {current.map((asset) => <option key={asset.asset_id} value={asset.asset_id}>{asset.name} · v{asset.version}</option>)}
      </select></label>
      {(versions.page > 1 || versions.data?.next_cursor != null) && <AssetPagination versions={versions} />}
      <label className="field">Mount path<input required placeholder="scripts/transform.lua" value={fileName} onChange={(event) => setFileName(event.target.value)} /></label>
      {fileName && <p className={overlap || !isAssetPath(fileName) ? 'error-text' : 'muted'}>
        {overlap ? 'This mount path overlaps another asset.' : !isAssetPath(fileName) ? 'Enter a relative file path.' : `/data/assets/${fileName}`}
      </p>}
      <div className="modal-actions">
        <button type="button" className="button" onClick={close}>Cancel</button>
        <button type="submit" className="button primary" disabled={!selected || !isAssetPath(fileName) || overlap}>{initial ? 'Save asset' : 'Add asset'}</button>
      </div>
    </form>
  </Modal>;
}
