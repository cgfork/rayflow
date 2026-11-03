import type { Graph, GraphNode } from './types';

export function createGraph(name: string): Graph {
  return { id: crypto.randomUUID(), name, version: 0, node_count: 1, nodes: [createNode('start')], edges: [] };
}
export function createNode(name: string): GraphNode {
  return { id: crypto.randomUUID(), name, node_type: 'docker', settings: { config: { image: '' }, resource: { cpu_cores: 1, memory_bytes: 268435456 }, timeout: '30s' }, inputs: [], outputs: [] };
}

export function normalizeGraph(graph: Graph): Graph {
  return { ...graph, node_count: graph.nodes.length, nodes: graph.nodes.map((node) => ({ ...node, settings: node.settings ?? { config: {}, resource: { cpu_cores: 0, memory_bytes: 0 }, timeout: '0s' } })) };
}

export function upstreamIds(graph: Graph, nodeId: string): Set<string> {
  const upstream = new Set<string>();
  const pending = [nodeId];
  while (pending.length) {
    const current = pending.pop()!;
    for (const [source, target] of graph.edges) {
      if (target === current && !upstream.has(source)) { upstream.add(source); pending.push(source); }
    }
  }
  return upstream;
}

export function outputChoices(graph: Graph, nodeId: string) {
  return graph.nodes.filter((producer) => producer.id !== nodeId && !upstreamIds(graph, producer.id).has(nodeId))
    .flatMap((producer) => producer.outputs.filter((output) => output.file_name.trim()).map((output) => ({ producer, output, source: `${producer.name}.${output.file_name}` })));
}

export function updateGraphNode(graph: Graph, updated: GraphNode): Graph {
  const old = graph.nodes.find((node) => node.id === updated.id);
  const references = new Map<string, string>();
  old?.outputs.forEach((output, index) => {
    const renamed = updated.outputs.find((item) => item.file_name === output.file_name)
      ?? (old.outputs.length === updated.outputs.length ? updated.outputs[index] : undefined);
    references.set(`${old.name}.${output.file_name}`, renamed ? `${updated.name}.${renamed.file_name}` : '');
  });
  return { ...graph, nodes: graph.nodes.map((node) => {
    const current = node.id === updated.id ? updated : node;
    return { ...current, inputs: current.inputs.map((input) => ({ ...input, source: references.get(input.source) ?? input.source })) };
  }) };
}

export function disconnectEdges(graph: Graph, edges: [string, string][]): Graph {
  const disconnected = { ...graph, edges: graph.edges.filter(([source, target]) => !edges.some(([from, to]) => from === source && to === target)) };
  return { ...disconnected, nodes: disconnected.nodes.map((node) => {
    const upstream = upstreamIds(disconnected, node.id);
    return { ...node, inputs: node.inputs.map((input) => ({ ...input, source: disconnected.nodes.some((producer) => upstream.has(producer.id) && producer.outputs.some((output) => input.source === `${producer.name}.${output.file_name}`)) ? input.source : '' })) };
  }) };
}

function durationMilliseconds(value: string): number {
  const units: Record<string, number> = { d: 86400000, h: 3600000, m: 60000, s: 1000, ms: 1 };
  const parts = [...value.trim().matchAll(/(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)/g)];
  if (!parts.length || value.trim().replace(/\d+(?:\.\d+)?\s*(?:ms|s|m|h|d)\s*/g, '')) return NaN;
  return parts.reduce((sum, part) => sum + Number(part[1]) * units[part[2]], 0);
}
export function validateGraph(graph: Graph) {
  const errors: string[] = [];
  const ids = new Set(graph.nodes.map((node) => node.id));
  if (!graph.name.trim()) errors.push('Give this workflow a name.');
  if (!graph.nodes.length) errors.push('Add at least one node.');
  if (graph.nodes.some((node) => !node.name.trim() || !node.node_type.trim())) errors.push('Every node needs a name and type.');
  if (graph.nodes.some((node) => node.node_type && node.node_type !== 'docker')) errors.push('Only Docker tasks are supported. Select Docker for every node.');
  if (new Set(graph.nodes.map((node) => node.name)).size !== graph.nodes.length) errors.push('Node names must be unique.');
  if (ids.size !== graph.nodes.length) errors.push('Node IDs must be unique.');
  for (const node of graph.nodes) {
    const settings = node.settings;
    if (!settings?.config || typeof settings.config !== 'object' || Array.isArray(settings.config)) errors.push(`${node.name}: Docker configuration must be a JSON object.`);
    if (typeof settings?.config?.image !== 'string' || !settings.config.image.trim()) errors.push(`${node.name}: Docker image is required.`);
    for (const field of ['command', 'env']) {
      const value = settings?.config?.[field];
      if (value !== undefined && (!Array.isArray(value) || value.some((item) => typeof item !== 'string'))) errors.push(`${node.name}: Docker ${field} must be an array of strings.`);
    }
    if (settings?.config?.working_dir !== undefined && typeof settings.config.working_dir !== 'string') errors.push(`${node.name}: Docker working_dir must be a string.`);
    if (!Number.isInteger(settings?.resource?.cpu_cores) || settings.resource.cpu_cores < 1 || settings.resource.cpu_cores > 4294967295) errors.push(`${node.name}: CPU cores must be a positive integer.`);
    if (!Number.isSafeInteger(settings?.resource?.memory_bytes) || settings.resource.memory_bytes < 1) errors.push(`${node.name}: Memory must be positive whole bytes within the supported range.`);
    const timeout = typeof settings?.timeout === 'string' ? durationMilliseconds(settings.timeout) : NaN;
    if (!Number.isSafeInteger(timeout) || timeout < 1) errors.push(`${node.name}: Timeout must be a positive duration such as 10s or 250ms.`);
    for (const [label, ports] of [['Input', node.inputs], ['Output', node.outputs]] as const) {
      if (ports.some((port) => !port.file_name.trim()) || new Set(ports.map((port) => port.file_name)).size !== ports.length) errors.push(`${node.name}: ${label} file names must be nonempty and unique.`);
    }
    const upstream = upstreamIds(graph, node.id);
    for (const input of node.inputs) {
      const producers = graph.nodes.filter((producer) => producer.outputs.some((output) => input.source === `${producer.name}.${output.file_name}`));
      if (producers.length !== 1 || !upstream.has(producers[0].id)) errors.push(`${node.name}: Input ${input.file_name || '(unnamed)'} must select an upstream output.`);
    }
  }
  if (graph.edges.some(([source, target]) => !ids.has(source) || !ids.has(target))) errors.push('An edge references a missing node.');
  const incoming = new Map(graph.nodes.map((node) => [node.id, 0]));
  for (const [, target] of graph.edges) incoming.set(target, (incoming.get(target) ?? 0) + 1);
  const roots = graph.nodes.filter((node) => incoming.get(node.id) === 0);
  if (graph.nodes.length && roots.length !== 1) errors.push(`A workflow needs exactly one root; found ${roots.length}.`);
  const queue = roots.map((node) => node.id);
  let visited = 0;
  while (queue.length) {
    const id = queue.shift()!;
    visited++;
    for (const [source, target] of graph.edges) {
      if (source !== id) continue;
      incoming.set(target, incoming.get(target)! - 1);
      if (incoming.get(target) === 0) queue.push(target);
    }
  }
  if (visited < graph.nodes.length) errors.push('Remove circular dependencies before publishing.');
  const referenced = new Set(graph.nodes.flatMap((node) => node.inputs.map((input) => input.source)));
  const unused = graph.nodes.flatMap((node) => node.outputs.filter((output) => !referenced.has(`${node.name}.${output.file_name}`)).map((output) => `${node.name}.${output.file_name}`));
  return { errors, unused };
}

export function layoutGraph(graph: Graph) {
  const levels = new Map(graph.nodes.map((node) => [node.id, 0]));
  for (let iteration = 0; iteration < graph.nodes.length; iteration++) {
    let changed = false;
    for (const [source, target] of graph.edges) {
      const next = Math.min(graph.nodes.length - 1, (levels.get(source) ?? 0) + 1);
      if (next > (levels.get(target) ?? 0)) { levels.set(target, next); changed = true; }
    }
    if (!changed) break;
  }
  const columns = new Map<number, number>();
  return graph.nodes.map((node) => {
    const level = levels.get(node.id) ?? 0;
    const row = columns.get(level) ?? 0;
    columns.set(level, row + 1);
    return { id: node.id, position: { x: level * 300 + 40, y: row * 160 + 80 }, data: { node } };
  });
}

export function removeNodes(graph: Graph, ids: string[]): Graph {
  const sources = new Set(graph.nodes.filter((node) => ids.includes(node.id)).flatMap((node) => node.outputs.map((output) => `${node.name}.${output.file_name}`)));
  return { ...graph,
    node_count: graph.nodes.filter((node) => !ids.includes(node.id)).length,
    nodes: graph.nodes.filter((node) => !ids.includes(node.id)).map((node) => ({ ...node, inputs: node.inputs.map((input) => ({ ...input, source: sources.has(input.source) ? '' : input.source })) })),
    edges: graph.edges.filter(([source, target]) => !ids.includes(source) && !ids.includes(target)),
  };
}
