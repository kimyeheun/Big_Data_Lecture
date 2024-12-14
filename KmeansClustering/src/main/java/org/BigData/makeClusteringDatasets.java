package org.BigData;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class makeClusteringDatasets {
    public static void main(String[] args) {
        // 파일 경로 설정
        String filePath = "KmeansClustering/kmean_input/dense_coordinates.txt";

        // 클러스터 중심점 설정 (예: 5개의 클러스터)
        double[][] centroids = {
                {20, 30}, // 클러스터 1 중심
                {60, 70}, // 클러스터 2 중심
                {20, 80}, // 클러스터 3 중심
                {80, 20}, // 클러스터 4 중심
                {50, 50}  // 클러스터 5 중심
        };

        // 클러스터당 생성할 데이터 수
        int pointsPerCluster = 2000;

        // Random 객체 생성
        Random random = new Random();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            // 각 클러스터 중심에 대해 밀집된 데이터 생성
            for (int i = 0; i < centroids.length; i++) {
                double centroidX = centroids[i][0];
                double centroidY = centroids[i][1];

                // 해당 클러스터에 데이터 포인트 2000개 생성
                for (int j = 0; j < pointsPerCluster; j++) {
                    // 중심을 기준으로 조금씩 변화하는 데이터를 생성
                    double x = centroidX + (random.nextGaussian() * 5);  // 가우시안 분포로 약간의 변동
                    double y = centroidY + (random.nextGaussian() * 5);  // 가우시안 분포로 약간의 변동

                    // 소수점 둘째자리까지만 표시
                    String formattedX = String.format("%.2f", x);
                    String formattedY = String.format("%.2f", y);

                    // 좌표를 파일에 작성 (x와 y를 탭으로 구분)
                    writer.write(formattedX + "\t" + formattedY);
                    writer.newLine();
                }
            }

            System.out.println("밀집된 데이터 생성 완료: " + filePath);

        } catch (IOException e) {
            System.err.println("파일 작성 중 오류가 발생했습니다: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

