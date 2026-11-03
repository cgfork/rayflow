import { useEffect, useMemo, useRef } from 'react';
import { Background, Controls, Handle, MarkerType, Position, ReactFlow, useNodesState, type Node, type NodeProps } from '@xyflow/react';
import { Box, ChevronRight, CircleCheck, CircleDot, Clock3 } from 'lucide-react';
import { disconnectEdges, layoutGraph, removeNodes } from './graph';
import type { Graph, GraphNode } from './types';

type TaskNode = Node<{ node: GraphNode; status?: string }, 'task'>;
function TaskCard({ data, selected }: NodeProps<TaskNode>) {
  const StatusIcon = data.status === 'Succeeded' ? CircleCheck : data.status === 'Running' ? CircleDot : Clock3;
  return <div className={`task-node ${selected ? 'selected' : ''} node-${data.status?.toLowerCase() ?? 'draft'}`}>
    <Handle type="target" position={Position.Left} />
    <div className="task-node-main"><span className="task-icon"><Box size={19} /></span><div><strong>{data.node.name || 'Unnamed node'}</strong><span>{data.node.node_type || 'script'}</span></div><ChevronRight size={14} /></div>
    <div className="task-node-footer">{data.status ? <><StatusIcon size={13} /><span>{data.status}</span></> : <><span>{data.node.inputs.length} inputs</span><span className="dot">·</span><span>{data.node.outputs.length} outputs</span></>}<span className="node-id">{data.node.id.slice(0, 6)}</span></div>
    <Handle type="source" position={Position.Right} />
  </div>;
}
const nodeTypes = { task: TaskCard };

export default function FlowCanvas({ graph, change, select, selected, statuses, layout = 0 }: {
  graph: Graph; change?: (graph: Graph) => void; select: (id: string | null) => void; selected?: string | null; statuses?: Record<string, string>; layout?: number;
}) {
  const [nodes, setNodes, onNodesChange] = useNodesState<TaskNode>([]);
  const previousLayout = useRef(layout);
  useEffect(() => {
    const reset = previousLayout.current !== layout;
    setNodes((previous) => layoutGraph(graph).map((node) => ({
      ...node, type: 'task', selected: node.id === selected,
      position: reset ? node.position : previous.find((existing) => existing.id === node.id)?.position ?? node.position,
      data: { node: node.data.node, status: statuses?.[node.id] },
    })));
    previousLayout.current = layout;
  }, [graph, selected, statuses, setNodes, layout]);
  const edges = useMemo(() => graph.edges.map(([source, target]) => ({
    id: `${source}:${target}`, source, target, type: 'smoothstep',
    markerEnd: { type: MarkerType.ArrowClosed, color: '#a6adbf', width: 17, height: 17 },
    animated: statuses?.[source] === 'Running',
    style: { stroke: statuses?.[source] === 'Succeeded' ? '#53a991' : '#a6adbf', strokeWidth: 1.6 },
  })), [graph.edges, statuses]);
  return <div className="flow-canvas" data-testid="flow-canvas"><ReactFlow<TaskNode>
    nodes={nodes} edges={edges} nodeTypes={nodeTypes} onNodesChange={onNodesChange}
    onNodeClick={(_, node) => select(node.id)} onPaneClick={() => select(null)}
    onConnect={change ? ({ source, target }) => {
      if (source !== target && !graph.edges.some((edge) => edge[0] === source && edge[1] === target)) change({ ...graph, edges: [...graph.edges, [source, target]] });
    } : undefined}
    onNodesDelete={change ? (removed) => change(removeNodes(graph, removed.map((node) => node.id))) : undefined}
    onEdgesDelete={change ? (removed) => change(disconnectEdges(graph, removed.map((edge) => [edge.source, edge.target]))) : undefined}
    nodesDraggable={!!change} nodesConnectable={!!change} edgesReconnectable={false} deleteKeyCode={change ? ['Backspace', 'Delete'] : null}
    fitView fitViewOptions={{ padding: 0.22, maxZoom: 1 }} minZoom={0.2} maxZoom={1.6}
  ><Background gap={22} size={1} color="#d3d8e4" /><Controls showInteractive={false} /></ReactFlow></div>;
}
