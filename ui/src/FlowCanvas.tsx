import { useEffect, useMemo, useRef } from 'react';
import { Background, ControlButton, Controls, Handle, MarkerType, Position, ReactFlow, useNodesState, useReactFlow, useStore, type Node, type NodeProps } from '@xyflow/react';
import { Box, ChevronRight, CircleCheck, CircleDot, Clock3, Maximize2, Minus, Plus } from 'lucide-react';
import { useTheme } from './Theme';
import { disconnectEdges, layoutGraph, removeNodes } from './graph';
import type { Graph, GraphNode } from './types';

type TaskNode = Node<{ node: GraphNode; status?: string }, 'task'>;
function TaskCard({ data, selected }: NodeProps<TaskNode>) {
  const StatusIcon = data.status === 'Succeeded' ? CircleCheck : data.status === 'Running' ? CircleDot : Clock3;
  return <div className={`task-node ${selected ? 'selected' : ''} node-${data.status?.toLowerCase() ?? 'draft'}`}>
    <Handle type="target" position={Position.Left} />
    <div className="task-node-main"><span className="task-icon"><Box size={19} /></span><div><strong>{data.node.name || 'Unnamed node'}</strong><span>{data.node.node_type || 'script'}</span></div><ChevronRight size={14} /></div>
    <div className="task-node-footer">{data.status ? <><StatusIcon size={13} /><span>{data.status}</span></> : <><span>{data.node.inputs.length} inputs</span><span className="dot">·</span><span>{data.node.outputs.length} outputs</span></>}<span className="node-id" title={data.node.id}>{data.node.id.slice(0, 6)}</span></div>
    <Handle type="source" position={Position.Right} />
  </div>;
}
const nodeTypes = { task: TaskCard };
const edgeColors = new Map([
  ['Running', 'var(--brand)'],
  ['Succeeded', 'var(--brand)'],
  ['Pending', 'var(--accent)'],
  ['Failed', 'var(--danger)'],
  ['Terminated', 'var(--danger)'],
]);

function CanvasControls() {
  const { zoomIn, zoomOut, fitView } = useReactFlow();
  const minZoomReached = useStore((state) => state.transform[2] <= state.minZoom);
  const maxZoomReached = useStore((state) => state.transform[2] >= state.maxZoom);
  const labels = useStore((state) => state.ariaLabelConfig);
  const zoomInLabel = labels['controls.zoomIn.ariaLabel'];
  const zoomOutLabel = labels['controls.zoomOut.ariaLabel'];
  const fitViewLabel = labels['controls.fitView.ariaLabel'];

  return <Controls showZoom={false} showFitView={false} showInteractive={false}>
    <ControlButton className="react-flow__controls-zoomin" title={zoomInLabel} aria-label={zoomInLabel} disabled={maxZoomReached} onClick={() => void zoomIn()}><Plus aria-hidden="true" /></ControlButton>
    <ControlButton className="react-flow__controls-zoomout" title={zoomOutLabel} aria-label={zoomOutLabel} disabled={minZoomReached} onClick={() => void zoomOut()}><Minus aria-hidden="true" /></ControlButton>
    <ControlButton className="react-flow__controls-fitview" title={fitViewLabel} aria-label={fitViewLabel} onClick={() => void fitView()}><Maximize2 aria-hidden="true" /></ControlButton>
  </Controls>;
}

export default function FlowCanvas({ graph, change, select, selected, statuses, layout = 0 }: {
  graph: Graph; change?: (graph: Graph) => void; select: (id: string | null) => void; selected?: string | null; statuses?: Record<string, string>; layout?: number;
}) {
  const { resolvedTheme } = useTheme();
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
  const edges = useMemo(() => graph.edges.map(([source, target]) => {
    const status = statuses?.[source];
    const color = edgeColors.get(status ?? '') ?? 'var(--muted)';
    return {
      id: `${source}:${target}`, source, target, type: 'smoothstep',
      markerEnd: { type: MarkerType.ArrowClosed, color, width: 17, height: 17 },
      animated: status === 'Running',
      style: { stroke: color, strokeWidth: 1.6 },
    };
  }), [graph.edges, statuses]);
  return <div className="flow-canvas" data-testid="flow-canvas"><ReactFlow<TaskNode>
    nodes={nodes} edges={edges} nodeTypes={nodeTypes} onNodesChange={onNodesChange} colorMode={resolvedTheme}
    onNodeClick={(_, node) => select(node.id)} onPaneClick={() => select(null)}
    onConnect={change ? ({ source, target }) => {
      if (source !== target && !graph.edges.some((edge) => edge[0] === source && edge[1] === target)) change({ ...graph, edges: [...graph.edges, [source, target]] });
    } : undefined}
    onNodesDelete={change ? (removed) => change(removeNodes(graph, removed.map((node) => node.id))) : undefined}
    onEdgesDelete={change ? (removed) => change(disconnectEdges(graph, removed.map((edge) => [edge.source, edge.target]))) : undefined}
    nodesDraggable={!!change} nodesConnectable={!!change} edgesReconnectable={false} deleteKeyCode={change ? ['Backspace', 'Delete'] : null}
    fitView fitViewOptions={{ padding: 0.22, maxZoom: 1 }} minZoom={0.2} maxZoom={1.6}
  ><Background gap={22} size={1} color="var(--canvas-grid)" /><CanvasControls /></ReactFlow></div>;
}
