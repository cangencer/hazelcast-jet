package com.hazelcast.jet.impl;

import com.hazelcast.jet.impl.execution.init.Diagnostics;
import com.hazelcast.jet.impl.execution.init.Diagnostics.EdgeD;

import javax.swing.*;
import java.awt.*;

import static javax.swing.WindowConstants.EXIT_ON_CLOSE;

public class Visualizer {

    private MainPanel mainPanel;

    public Visualizer(Diagnostics diagnostics) {
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        SwingUtilities.invokeLater(() -> {
            mainPanel = new MainPanel(diagnostics);
            buildFrame(mainPanel);
            new Timer(50, e -> mainPanel.update()).start();
        });
    }

    private static void buildFrame(MainPanel mainPanel) {
        mainPanel.setBackground(Color.WHITE);
        final JFrame frame = new JFrame();
        frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
        frame.setTitle("Jet Visualizer");
        frame.setBounds(40, 10, 750, 1000);
        frame.setLayout(new BorderLayout());
        frame.add(mainPanel);
        frame.setVisible(true);
    }

    private static class MainPanel extends JPanel {
        private final Diagnostics diagnostics;
        private final VisualizerImage image = new VisualizerImage();

        MainPanel(Diagnostics diagnostics) {
            this.diagnostics = diagnostics;
        }

        @Override
        protected void paintComponent(Graphics g) {
            image.paint((Graphics2D) g);
        }

        void update() {
            for (EdgeD edgeD : diagnostics.edges.values()) {
                int prctFull = edgeD.localInFlightItems();
                Color color =
                          prctFull > 90 ? Color.RED
                        : prctFull > 50 ? Color.ORANGE
                        : prctFull > 25 ? Color.PINK
                        : prctFull > 5 ? Color.BLUE
                        : Color.BLACK;
                image.setPropertiesFor(edgeD.name(), 2, color);
            }
            repaint();
        }
    }
}
