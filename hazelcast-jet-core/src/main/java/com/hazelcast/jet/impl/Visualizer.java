package com.hazelcast.jet.impl;/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.hazelcast.jet.impl.execution.init.Diagnostics;
import com.hazelcast.jet.impl.execution.init.Diagnostics.EdgeD;

import javax.swing.*;
import java.awt.*;
import java.util.HashMap;
import java.util.Map;

public class Visualizer extends JFrame {

    private final float scale = UIManager.getDefaults().getFont("TextField.font").getSize() / 12f;

    public static final int THICKNESS_NARROW = 1;
    public static final int THICKNESS_MIDDLE = 1;
    public static final int THICKNESS_THICK = 1;
    private final Diagnostics diagnostics;

    private final class MyPanel extends JPanel {

        private final Map<String, Edge> edges = new HashMap<>();

        @Override
        protected void paintComponent(Graphics g) {
            super.paintComponent(g);
            int[] y = {0};
            edges.forEach((name, edge) -> {
                g.drawRect(20, 400 - y[0], edge.thickness, 100);
                y[0] += 20;
            });
        }

        private void update() {
            for (EdgeD edgeD : diagnostics.edges.values()) {
                int prctFull = edgeD.localInFlightItems();
                Color color = prctFull == 100 ? Color.RED : Color.BLUE;
                setEdgeProperties(edgeD.name(), color, 2);
            }
        }

        private void setEdgeProperties(String edgeName, Color color, int thickness) {
            System.out.println("edge " + edgeName);
            edges.computeIfAbsent(edgeName, x -> new Edge())
                 .update(color, thickness);
        }

    }


    public Visualizer(Diagnostics diagnostics) throws HeadlessException {
        this.diagnostics = diagnostics;
    }

    public void startVisualizing() {
        SwingUtilities.invokeLater(() -> {
            setTitle("Jet Visualizer");
            setLayout(new BorderLayout());
            setSize((int) (1024*scale), (int) (768*scale));
            setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

            MyPanel container = new MyPanel();
            container.setSize((int) (2000*scale), (int) (2000*scale));
            container.setMaximumSize(new Dimension((int) (2000*scale), (int) (2000*scale)));
            JScrollPane scrPane = new JScrollPane(container);
            this.add(scrPane);
            scrPane.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);

            setVisible(true);

            System.out.println(scrPane.getSize());

            new Timer(100, e -> {
                container.update();
            }).start();
        });
    }


    private static class Edge {
        Color color;
        int thickness;

        void update(Color color, int thickness) {
            this.color = color;
            this.thickness = thickness;
        }
    }
}
