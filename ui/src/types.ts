export interface Input { file_name: string; source: string; required: boolean; format: string | null }
export interface Output { file_name: string; kind: 'File' | 'Directory'; multiple: boolean }
export interface NodeSettings { config: Record<string, unknown>; resource: { cpu_cores: number; memory_bytes: number }; timeout: string }
export interface GraphNode { id: string; name: string; node_type: string; settings: NodeSettings; inputs: Input[]; outputs: Output[] }
export interface Graph { id: string; name: string; version: number; node_count: number; nodes: GraphNode[]; edges: [string, string][] }
export interface Page<T> { items: T[]; next_cursor: number | null }
export interface Run { id: number; uuid: string; data_id: string; graph_id: string; version: number; revision: number; created_at: string; updated_at: string }
export type NodeStatus = 'Ready' | 'Running' | 'Pending' | 'Succeeded' | 'Failed' | 'Terminated';
export interface NodeState extends Omit<GraphNode, 'id'> { node_id: string; status: NodeStatus; revision: number; updated_at: string }
export interface RunView { state: { uuid: string; data_id: string; graph_id: string; version: number; revision: number; nodes: NodeState[] }; graph: Graph; current: boolean }
export type JobStatus = 'Scheduled' | 'Polled' | 'Yielded' | 'Succeeded' | 'Failed' | 'Terminated';
export interface Job { id: number; uuid: string; task_id: string; run_id: string; node_run_id: number; status: JobStatus; required_cpu_cores: number; required_memory_bytes: number; agent_id: string | null; polled_at: string | null; expired_at: string | null; created_at: string; updated_at: string }
export interface Agent { id: number; uuid: string; cpu_cores: number; memory_bytes: number; active_jobs: number; first_seen_at: string; last_seen_at: string; online_until: string; online: boolean }
export type Section = 'orchestration' | 'runtime' | 'schedule' | 'agents';
