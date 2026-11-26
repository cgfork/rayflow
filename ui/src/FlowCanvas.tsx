import { useCallback, useEffect, useMemo, useRef } from 'react';
import { Background, ControlButton, Controls, Handle, MarkerType, Position, ReactFlow, useNodesState, useReactFlow, useStore, type Node, type NodeProps, type OnSelectionChangeParams } from '@xyflow/react';
import { Box, ChevronRight, CircleCheck, CircleDot, Clock3, Maximize2, Minus, Plus } from 'lucide-react';
import { useTheme } from './Theme';
import { disconnectEdges, layoutGraph, removeNodes } from './graph';
import type { Graph, GraphNode } from './types';

export type CanvasChange = 'added' | 'modified' | 'removed';
export type CanvasDiff = {
  nodes: ReadonlyMap<string, CanvasChange>;
  edges: ReadonlyMap<string, 'added' | 'removed'>;
};

const changeLabels: Record<CanvasChange, string> = { added: 'Added', modified: 'Modified', removed: 'Removed' };
const changeColors = { added: 'var(--success)', removed: 'var(--danger)' };

type TaskNode = Node<{ node: GraphNode; status?: string; change?: CanvasChange; comparison?: boolean }, 'task'>;
function TaskCard({ data, selected }: NodeProps<TaskNode>) {
  const StatusIcon = data.status === 'Succeeded' ? CircleCheck : data.status === 'Running' ? CircleDot : Clock3;
  return <div className={`task-node ${selected ? 'selected' : ''} node-${data.status?.toLowerCase() ?? 'draft'}`} data-change={data.change}>
    <Handle type="target" position={Position.Left} />
    <div className="task-node-main"><span className="task-icon"><Box size={19} /></span><div><strong>{data.node.name || 'Unnamed node'}</strong><span>{data.node.node_type || 'script'}</span></div><ChevronRight size={14} /></div>
    <div className="task-node-footer">
      {data.change ? <span>{changeLabels[data.change]}</span>
        : data.status ? <><StatusIcon size={13} /><span>{data.status}</span></>
          : <><span>{data.node.inputs.length} inputs</span><span className="dot">·</span><span>{data.node.outputs.length} outputs</span></>}
      <span className="node-id" title={data.node.id}>{data.node.id.slice(0, 6)}</span>
    </div>
    <Handle type="source" position={Position.Right} />
    {data.comparison && <>
      <Handle id="top" type="source" position={Position.Top} className="graph-diff-handle" />
      <Handle id="top" type="target" position={Position.Top} className="graph-diff-handle" />
    </>}
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

export default function FlowCanvas({ graph, change, select, selected, statuses, diff, layout = 0 }: {
  graph: Graph; change?: (graph: Graph) => void; select: (id: string | null) => void; selected?: string | null;
  statuses?: Record<string, string>; diff?: CanvasDiff; layout?: number;
}) {
  const { resolvedTheme } = useTheme();
  const [nodes, setNodes, onNodesChange] = useNodesState<TaskNode>([]);
  const previousLayout = useRef(layout);
  const layoutNodes = useMemo(() => {
    if (!diff) return layoutGraph(graph);
    // Former connections only place removed nodes, so the target layout stays acyclic.
    const edges = graph.edges.filter(([source, target]) => (
      diff.edges.get(`${source}:${target}`) !== 'removed' || diff.nodes.get(target) === 'removed'
    ));
    return layoutGraph({ ...graph, edges });
  }, [diff, graph]);
  useEffect(() => {
    const reset = previousLayout.current !== layout;
    setNodes((previous) => layoutNodes.map((node) => {
      const kind = diff?.nodes.get(node.id);
      return {
        ...node, type: 'task', selected: node.id === selected,
        position: reset ? node.position : previous.find((existing) => existing.id === node.id)?.position ?? node.position,
        ariaLabel: diff ? `${node.data.node.name}, ${kind ? changeLabels[kind] : 'Unchanged'}` : undefined,
        data: { node: node.data.node, status: statuses?.[node.id], change: kind, comparison: !!diff },
      };
    }));
    previousLayout.current = layout;
  }, [layoutNodes, selected, statuses, diff, setNodes, layout]);
  const edges = useMemo(() => {
    const byId = diff ? new Map(layoutNodes.map((node) => [node.id, node])) : undefined;
    return graph.edges.map(([source, target]) => {
      const id = `${source}:${target}`;
      const kind = diff?.edges.get(id);
      const status = statuses?.[source];
      const from = byId?.get(source);
      const to = byId?.get(target);
      const backwards = kind === 'removed' && from && to && from.position.x > to.position.x;
      const color = kind ? changeColors[kind] : edgeColors.get(status ?? '') ?? 'var(--muted)';
      return {
        id, source, target, type: 'smoothstep',
        sourceHandle: backwards ? 'top' : undefined,
        targetHandle: backwards ? 'top' : undefined,
        ariaLabel: kind ? `${changeLabels[kind]} connection from ${from?.data.node.name ?? source} to ${to?.data.node.name ?? target}` : undefined,
        selectable: diff ? false : undefined,
        focusable: diff ? false : undefined,
        markerEnd: { type: MarkerType.ArrowClosed, color, width: 17, height: 17 },
        animated: status === 'Running',
        style: { stroke: color, strokeWidth: kind ? 2.6 : 1.6, strokeDasharray: kind === 'removed' ? '6 4' : undefined },
      };
    });
  }, [graph.edges, layoutNodes, statuses, diff]);
  const selectComparisonNode = useCallback(({ nodes }: OnSelectionChangeParams<TaskNode>) => {
    select(nodes[0]?.id ?? null);
  }, [select]);
  return <div className="flow-canvas" data-testid="flow-canvas"><ReactFlow<TaskNode>
    nodes={nodes} edges={edges} nodeTypes={nodeTypes} onNodesChange={onNodesChange} colorMode={resolvedTheme}
    onSelectionChange={diff ? selectComparisonNode : undefined}
    onNodeClick={diff ? undefined : (_, node) => select(node.id)} onPaneClick={diff ? undefined : () => select(null)}
    onConnect={change ? ({ source, target }) => {
      if (source !== target && !graph.edges.some((edge) => edge[0] === source && edge[1] === target)) change({ ...graph, edges: [...graph.edges, [source, target]] });
    } : undefined}
    onNodesDelete={change ? (removed) => change(removeNodes(graph, removed.map((node) => node.id))) : undefined}
    onEdgesDelete={change ? (removed) => change(disconnectEdges(graph, removed.map((edge) => [edge.source, edge.target]))) : undefined}
    nodesDraggable={!!change} nodesConnectable={!!change} edgesReconnectable={false} deleteKeyCode={change ? ['Backspace', 'Delete'] : null}
    selectionKeyCode={diff ? null : undefined} multiSelectionKeyCode={diff ? null : undefined}
    zoomOnScroll={!diff} preventScrolling={!diff}
    fitView fitViewOptions={{ padding: 0.22, maxZoom: 1 }} minZoom={0.2} maxZoom={1.6}
  ><Background gap={22} size={1} color="var(--canvas-grid)" /><CanvasControls /></ReactFlow></div>;
}
