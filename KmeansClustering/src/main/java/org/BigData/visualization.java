package org.BigData;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.swing.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class visualization {
    public static void main(String[] args) {
        // 파일 경로 설정
        String filePath = "KmeansClustering/kmean_input/dense_coordinates.txt";

        // XYSeries 생성
        XYSeries series = new XYSeries("Dataset");

        // 파일에서 데이터를 읽어서 좌표를 추가
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] coordinates = line.split("\t");
                if (coordinates.length == 2) {
                    try {
                        double x = Double.parseDouble(coordinates[0]);
                        double y = Double.parseDouble(coordinates[1]);
                        series.add(x, y);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid data format: " + line);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("파일 읽기 중 오류가 발생했습니다: " + e.getMessage());
            e.printStackTrace();
        }

        // XYSeriesCollection에 데이터 추가
        XYSeriesCollection dataset = new XYSeriesCollection(series);

        // 차트 생성
        JFreeChart chart = ChartFactory.createXYLineChart(
                "Dataset Visualization", // 차트 제목
                "X", // X축 라벨
                "Y", // Y축 라벨
                dataset, // 데이터셋
                PlotOrientation.VERTICAL, // 차트 방향
                true, // 범례 표시
                true, // 툴팁 표시
                false // URL 표시
        );

        // XYPlot을 얻고, 점만 표시하도록 설정
        XYPlot plot = chart.getXYPlot();
        XYItemRenderer renderer = plot.getRenderer();
        if (renderer instanceof XYLineAndShapeRenderer) {
            XYLineAndShapeRenderer shapeRenderer = (XYLineAndShapeRenderer) renderer;
            shapeRenderer.setSeriesLinesVisible(0, false);  // 선 숨기기
            shapeRenderer.setSeriesShapesVisible(0, true);  // 점 표시
        }

        // 차트를 패널에 넣어서 표시
        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(800, 600));

        // JFrame에 차트를 추가하여 표시
        JFrame frame = new JFrame("Dataset Visualization");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().add(chartPanel);
        frame.pack();
        frame.setVisible(true);
    }
}
