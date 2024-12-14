package org.BigData;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class makeDatasets {
    public static void main(String[] args) {
        // 파일 경로 설정
        String filePath = "KmeansClustering/kmean_input/coordinates.txt";

        // Random 객체 생성
        Random random = new Random();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            // 10,000개의 좌표 생성
            for (int i = 0; i < 10000; i++) {
                // 0에서 200 사이의 랜덤한 double 값 생성
                double x = random.nextDouble() * 100;
                double y = random.nextDouble() * 100;

                // 소수점 둘째자리까지만 표시
                String formattedX = String.format("%.2f", x);
                String formattedY = String.format("%.2f", y);

                // 좌표를 파일에 작성 (x와 y를 탭으로 구분)
                writer.write(formattedX + "\t" + formattedY);
                writer.newLine();
            }

            System.out.println("파일 생성이 완료되었습니다: " + filePath);

        } catch (IOException e) {
            System.err.println("파일 작성 중 오류가 발생했습니다: " + e.getMessage());
            e.printStackTrace();
        }
    }
}