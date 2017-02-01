package com.hazelcast.jet.impl;

import com.hazelcast.jet.impl.execution.init.Diagnostics;
import com.hazelcast.jet.impl.execution.init.Diagnostics.EdgeD;

import javax.swing.*;
import java.awt.*;
import java.util.HashMap;
import java.util.Map;

import static javax.swing.WindowConstants.EXIT_ON_CLOSE;

public class Visualizer {

    private MainPanel mainPanel;

    public Visualizer(Diagnostics diagnostics) {
        SwingUtilities.invokeLater(() -> {
            mainPanel = new MainPanel(diagnostics);
            buildFrame(mainPanel);
            new Timer(100, e -> mainPanel.update()).start();
        });
    }

    private static void buildFrame(MainPanel mainPanel) {
        final JFrame frame = new JFrame();
        frame.setBackground(Color.WHITE);
        frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
        frame.setTitle("Jet Visualizer");
        frame.setBounds(40, 40, 1000, 600);
        frame.setLayout(new BorderLayout());
        frame.add(mainPanel);
        frame.setVisible(true);
    }

    private static class MainPanel extends JPanel {
        private final Diagnostics diagnostics;
        private final Map<String, Edge> edges = new HashMap<>();

        MainPanel(Diagnostics diagnostics) {
            this.diagnostics = diagnostics;
        }

        @Override
        protected void paintComponent(Graphics g) {
            super.paintComponent(g);
            g.setColor(getBackground());
            g.fillRect(0, 0, getWidth(), getHeight());
            int[] y = {0};
            edges.forEach((name, edge) -> {
                g.setColor(edge.color);
                g.fillRect(20, 400 - y[0], 100, edge.thickness);
                y[0] += 20;
            });
        }

        void update() {
            for (EdgeD edgeD : diagnostics.edges.values()) {
                int prctFull = edgeD.localInFlightItems();
                Color color = prctFull == 100 ? Color.RED : Color.BLUE;
                edges.computeIfAbsent(edgeD.name(), x -> new Edge())
                     .update(color, 2);
            }
            repaint();
        }

    }

    private static class Edge {
        Color color = Color.BLACK;
        int thickness = 2;

        void update(Color color, int thickness) {
            this.color = color;
            this.thickness = thickness;
        }

        @Override
        public String toString() {
            return color + " " + thickness;
        }
    }
}
