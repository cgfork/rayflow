export interface Input { file_name: string; source: string; required: boolean; format: string | null }
export interface Output { file_name: string; kind: 'File' | 'Directory' }
interface NodeResource { cpu_cores: number; memory_bytes: number }
export interface NodeSettings {
  config: Record<string, unknown>;
  resource: NodeResource;
  timeout: string;
  overrides?: Record<string, AnnotationMatchNodeSettings>;
}
export interface AnnotationMatchNodeSettings {
  annotation_match?: string | null;
  config?: Record<string, unknown> | null;
  resource?: NodeResource | null;
  timeout?: string | null;
}
export interface AssetRef { file_name: string; asset_id: string }
export interface AssetVersion {
  asset_id: string;
  name: string;
  version: number;
  size: number;
  format: string;
  sha256: string;
  created_by: string;
  created_at: string;
  published_at: string;
}
export interface PublishedAsset extends AssetVersion { catalog: Catalog }
export type PrepareAssetResponse =
  | { state: 'pending'; asset_id: string; upload_id: string; catalog: Catalog; upload: PresignedUrl<'upload'> }
  | (PublishedAsset & { state: 'published' });
export interface GraphNode { id: string; name: string; node_type: string; settings: NodeSettings; inputs: Input[]; outputs: Output[]; assets: AssetRef[] }
export interface Graph {
  id: string;
  name: string;
  version: number;
  node_count: number;
  nodes: GraphNode[];
  edges: [string, string][];
  created_by?: string;
  created_at?: string;
  updated_by?: string;
  updated_at?: string;
}
export interface Page<T> { items: T[]; next_cursor: number | null }
export interface Run { id: number; uuid: string; data_id: string; graph_id: string; version: number; revision: number; created_at: string; updated_at: string }
export type NodeStatus = 'Ready' | 'Running' | 'Pending' | 'Succeeded' | 'Failed' | 'Terminated';
export interface NodeRun {
  id: number;
  uuid: string;
  run_id: string;
  data_id: string;
  graph_id: string;
  version: number;
  node_id: string;
  revision: number;
  status: NodeStatus;
  ineffective: number;
  created_at: string;
  updated_at: string;
}
export interface RunView { run: Run; graph: Graph; node_runs: [NodeRun, GraphNode][] }
export interface Catalog {
  id: number;
  uuid: string;
  key: string;
  url: string;
  size: number;
  format: string;
  uploaded: boolean;
  created_at: string;
  updated_at: string;
}
interface PresignedUrl<Operation extends 'upload' | 'download'> {
  url: string;
  operation: Operation;
  headers: Record<string, string>;
  expires_at: string;
}
interface CatalogDownload {
  catalog: Catalog;
  presigned: PresignedUrl<'download'>;
  multipart: boolean;
}
export interface AssetTransfer extends CatalogDownload { asset_id: string; sha256: string }
export type RunFileTransfer = CatalogDownload;
export interface RunArchiveTransfer extends PresignedUrl<'download'> { catalog: Catalog }
export type JobStatus = 'Scheduled' | 'Polled' | 'Yielded' | 'Succeeded' | 'Failed' | 'Terminated';
export interface Job { id: number; uuid: string; task_id: string; run_id: string; node_run_id: number; status: JobStatus; required_cpu_cores: number; required_memory_bytes: number; agent_id: string | null; polled_at: string | null; expired_at: string | null; created_at: string; updated_at: string }
export interface Agent { id: number; uuid: string; cpu_cores: number; memory_bytes: number; active_jobs: number; first_seen_at: string; last_seen_at: string; online_until: string; online: boolean }
export type Section = 'orchestration' | 'assets' | 'runtime' | 'schedule' | 'agents';
