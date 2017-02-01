package com.hazelcast.jet.impl;

import com.hazelcast.jet.impl.execution.init.Diagnostics;
import com.hazelcast.jet.impl.execution.init.Diagnostics.DiagData;
import com.hazelcast.jet.impl.execution.init.EdgeDef;
import com.hazelcast.jet.impl.execution.init.VertexDef;
import com.yworks.yfiles.geometry.PointD;
import com.yworks.yfiles.geometry.RectD;
import com.yworks.yfiles.geometry.SizeD;
import com.yworks.yfiles.graph.IEdge;
import com.yworks.yfiles.graph.IGraph;
import com.yworks.yfiles.graph.ILabel;
import com.yworks.yfiles.graph.INode;
import com.yworks.yfiles.graph.LayoutUtilities;
import com.yworks.yfiles.graph.styles.Arrow;
import com.yworks.yfiles.graph.styles.IEdgeStyle;
import com.yworks.yfiles.graph.styles.PolylineEdgeStyle;
import com.yworks.yfiles.graph.styles.ShapeNodeShape;
import com.yworks.yfiles.graph.styles.ShapeNodeStyle;
import com.yworks.yfiles.graph.styles.SimpleLabelStyle;
import com.yworks.yfiles.layout.hierarchic.HierarchicLayout;
import com.yworks.yfiles.view.Colors;
import com.yworks.yfiles.view.GraphComponent;
import com.yworks.yfiles.view.Pen;

import javax.swing.*;
import java.awt.*;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;

public class Visualizer {


    public Visualizer(java.util.List<VertexDef> vertices, Diagnostics diagnostics) {
        JFrame frame = new JFrame("Jet DAG Visualizer");
        frame.setSize(1024, 768);
        frame.setLocationRelativeTo(null);
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        frame.setVisible(true);

        GraphComponent graphComponent = new GraphComponent();
        frame.add(graphComponent, BorderLayout.CENTER);

        HierarchicLayout layout = new HierarchicLayout();
        layout.setMinimumLayerDistance(80);

        IGraph graph = graphComponent.getGraph();

        ShapeNodeStyle nodeStyle = new ShapeNodeStyle();
        nodeStyle.setShape(ShapeNodeShape.ROUND_RECTANGLE);
        nodeStyle.setPaint(Colors.LIGHT_BLUE);

        Map<String, INode> nodes = new HashMap<>();
        Map<String, IEdge> edges = new HashMap<>();
        for (VertexDef vertex : vertices) {
            INode node = graph.createNode(new RectD(new PointD(0, 0), new SizeD(120, 40)));
            graph.addLabel(node, vertex.name());
            graph.setStyle(node, nodeStyle);
            nodes.put(vertex.name(), node);
        }

        for (VertexDef vertex : vertices) {
            for (EdgeDef edgeDef : vertex.outboundEdges()) {
                String name = edgeDef.sourceVertex().name() + "->" + edgeDef.destVertex().name();
                IEdge edge = graph.createEdge(nodes.get(edgeDef.sourceVertex().name()),
                        nodes.get(edgeDef.destVertex().name()));
                edges.put(name, edge);
            }
        }

        graphComponent.fitGraphBounds();

        LayoutUtilities.morphLayout(graphComponent, layout, Duration.ZERO, null);

        PolylineEdgeStyle orange = getStyle(Colors.ORANGE);
        PolylineEdgeStyle red = getStyle(Colors.RED);
        PolylineEdgeStyle pink = getStyle(Colors.PINK);
        PolylineEdgeStyle blue = getStyle(Colors.BLUE);
        PolylineEdgeStyle black = getStyle(Colors.BLACK);
        PolylineEdgeStyle gray = getStyle(Colors.GRAY);

        new Timer(50, e -> {
            for (Entry<String, DiagData> entry : diagnostics.aggrData().entrySet()) {

                DiagData value = entry.getValue();
                int prctFull = IntStream.of(value.localUtilization, value.receiverUtilization, value.senderUtilization)
                                        .max().orElse(0);
                IEdgeStyle style =
                        prctFull > 90 ? red
                                : prctFull > 30 ? orange
                                : prctFull > 15 ? pink
                                : prctFull > 5 ? blue
                                : prctFull >= 0 ? gray
                                : black;

                graph.setStyle(edges.get(entry.getKey()), style);
                graphComponent.repaint();
            }
        }).start();
    }

    private PolylineEdgeStyle getStyle(Paint color) {
        PolylineEdgeStyle orange = new PolylineEdgeStyle();
        Pen pen = new Pen(color);
        pen.setThickness(3);
        orange.setPen(pen);
        orange.setTargetArrow(Arrow.DEFAULT);
        return orange;
    }
}
