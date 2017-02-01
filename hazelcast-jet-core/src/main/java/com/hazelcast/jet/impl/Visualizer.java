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

import javax.swing.*;
import java.awt.*;

public class Visualizer extends JFrame {

    private final float scale = UIManager.getDefaults().getFont("TextField.font").getSize() / 12f;

    public static final int THICKNESS_NARROW = 1;
    public static final int THICKNESS_MIDDLE = 1;
    public static final int THICKNESS_THICK = 1;

    /**
     * @param edgeName "&lt;source_vertex_name>-&lt;target_vertex_name>"
     * @param color
     * @param thickness {@code THICKNESS_*} constants
     */
    public void setEdgeProperties(String edgeName, Color color, int thickness) {
        // TODO
    }

    private final class MyPanel extends JPanel {
        @Override
        protected void paintComponent(Graphics g) {
            super.paintComponent(g);

            ((Graphics2D) g).scale(scale, scale);

            g.drawRect(50, 50, 1950, 1950);
        }
    }

    public Visualizer() {
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
    }

    public static void main(String[] args) throws ClassNotFoundException, UnsupportedLookAndFeelException, InstantiationException, IllegalAccessException {
        UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        new Visualizer();
    }

}
