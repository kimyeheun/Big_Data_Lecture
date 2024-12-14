package org.BigData;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.*;


public class KmeansClustering extends Configured implements Tool {

    public static int k;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new KmeansClustering(), args);
    }

//    public static class KmeanSet implements WritableComparable<KmeanSet> {
//        DoubleWritable x;
//        DoubleWritable y;
//
//        public KmeanSet() {}
//
//        public KmeanSet(double x, double y) {
//            this.x = new DoubleWritable(x);
//            this.y = new DoubleWritable(y);
//        }
//
//        public double getX(){
//            return Double.parseDouble(this.x.toString());
//        }
//
//        public double getY(){
//            return Double.parseDouble(this.y.toString());
//        }
//
//        public String toString(){
//            return this.x.toString() + "," + this.y.toString();
//        }
//
//        @Override
//        public int compareTo(KmeanSet kmeanSet) {
//            return 0;
//        }
//
//        @Override
//        public void write(DataOutput dataOutput) throws IOException {
//            dataOutput.writeDouble(x);
//            dataOutput.writeDouble(y);
//        }
//
//        @Override
//        public void readFields(DataInput dataInput) throws IOException {
//
//        }
//    }

    public static class KmeanSet implements WritableComparable<KmeanSet> {
        private double x;
        private double y;

        // 기본 생성자 (Hadoop에서 필요)
        public KmeanSet() {}

        // 사용자 정의 생성자
        public KmeanSet(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public double getX() {
            return x;
        }

        public double getY() {
            return y;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            if (Double.isNaN(x) || Double.isNaN(y)) {
                throw new IOException("NaN");
            }
            out.writeDouble(x);
            out.writeDouble(y);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            x = in.readDouble();
            y = in.readDouble();
            if (Double.isNaN(x) || Double.isNaN(y)) {
                throw new IOException("Nan");
            }
        }

        @Override
        public int compareTo(KmeanSet other) {
            int cmp = Double.compare(this.x, other.x);
            if (cmp != 0) {
                return cmp;
            }
            return Double.compare(this.y, other.y);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            KmeanSet other = (KmeanSet) obj;
            return Double.compare(x, other.x) == 0 && Double.compare(y, other.y) == 0;
        }

        @Override
        public int hashCode() {
            return Double.hashCode(x) * 31 + Double.hashCode(y);
        }

        @Override
        public String toString() {
            return x + "," + y;
        }
    }

    /**
     * 이름:
     *
     * input :
     * return :
     */
    public static class KmeansMap extends Mapper<Object, Text, KmeanSet, KmeanSet> {
        Set<KmeanSet> centroids =  new HashSet<>();
        /***
         *
         * Class MAPPER
         *  method INITIALIZE
         *      L ← new List()
         *      Table T ← DistributedCache
         *      for all centroid k ∈ T do
         *          L←k
         *  method MAP (centroid cur_k , clusters c )
         *      int d[]
         *      for all centroid k ∈ L do
         *          d []← Distance(k, c)
         *      cur_k ← k of smallest distance
         *      EMIT (centroid cur_k, clusters c )
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                Path filePath = new Path(cacheFiles[0].getPath());
//                BufferedReader reader = new BufferedReader(new FileReader(filePath.getName()));
                BufferedReader reader = new BufferedReader(new FileReader(filePath.toString())); // 로컬
                String line;
                while ((line = reader.readLine()) != null) {
                    // <key, value> 분류
                    String[] pair = line.split("\\s+");

                    String[] coordinate = pair[0].split(",");
                    double x = Double.parseDouble(coordinate[0].trim());
                    double y = Double.parseDouble(coordinate[1].trim());
                    centroids.add(new KmeanSet(x, y));
                }
                System.out.println("centeroid num: " + centroids.size());
                reader.close();
            }
        }

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, KmeanSet, KmeanSet>.Context context)
                throws IOException, InterruptedException {

            List<KmeanSet> centroidsList = new ArrayList<>(centroids);

            String[] coordinates = value.toString().split("\\s+");
            double pointX = Double.parseDouble(coordinates[0]);
            double pointY = Double.parseDouble(coordinates[1]);
            KmeanSet point = new KmeanSet(pointX, pointY);

            // k개의 중심점과 각 좌표 간의 거리 리스트
            List<Double> kDistanceWithCentroid = new ArrayList<>();

            // 거리 계산
            for (KmeanSet centroid : centroids) {
                double distance = calculateDistance(point, centroid);
                kDistanceWithCentroid.add(distance);
            }
            // 가장 작은 중심점과의 거리를 key 값으로 설정
            int indexOfMin = getIndexOfMin(kDistanceWithCentroid);
            context.write(centroidsList.get(indexOfMin), point);
        }

        private double calculateDistance(KmeanSet point1, KmeanSet point2) {
            double dx = point1.getX() - point2.getX();
            double dy = point1.getY() - point2.getY();
            return Math.sqrt(dx * dx + dy * dy);
        }

        private static int getIndexOfMin(List<Double> distances) {
            int minIndex = 0;
            for (int i = 1; i < distances.size(); i++) {
                if (distances.get(i) < distances.get(minIndex)) {
                    minIndex = i;
                }
            }
            return minIndex;
        }
    }

    public static class KmeansMapFirst extends Mapper<Object, Text, KmeanSet, KmeanSet> {
        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, KmeanSet, KmeanSet>.Context context)
                throws IOException, InterruptedException {

            StringTokenizer st = new StringTokenizer(value.toString());
            while (st.hasMoreTokens()) {
                context.write(new KmeanSet(0,0),
                        new KmeanSet(Double.parseDouble(st.nextToken()),
                        Double.parseDouble(st.nextToken())));
            }
        }
    }

    public static class KmeansReduce extends Reducer<KmeanSet, KmeanSet, Text, Text> {

        @Override
        protected void reduce(KmeanSet key, Iterable<KmeanSet> values,
                              Reducer<KmeanSet, KmeanSet, Text, Text>.Context context)
                throws IOException, InterruptedException {

            // 모든 클러스터 <x, y>
            List<KmeanSet> xYs = new ArrayList<>();

            for (KmeanSet cluster: values) {
                xYs.add(new KmeanSet(cluster.getX(), cluster.getY()));
            }

            int len = xYs.size();
            if (len == 10000) {
                // 임의로 구역을 나눠, k 개의 centroid를 찾음
                // 1. k 개의 클러스터를 담을 리스트 생성
                List<List<KmeanSet>> clusters = new ArrayList<>();
                for (int i = 0; i < k; i++) {
                    clusters.add(new ArrayList<>());
                }

//                System.out.println("xys : " + xYs.get(0));
//                System.out.println("xys : " + xYs.get(1));
//                System.out.println("xys : " + xYs.get(2));
//                System.out.println("xys : " + xYs.get(3));
                // 데이터를 k개의 구역으로 나누기
                for (int i = 0; i < len; i++) {
                    clusters.get(i % k).add(xYs.get(i));
                }

//                System.out.println("cluster - xys : " + 0+ "- " + clusters.get(0).subList(0, 10));
//                System.out.println("cluster - xys : " + 0+ "- " + clusters.get(1).subList(0, 10));
//                System.out.println("cluster - xys : " + 0+ "- " + clusters.get(2).subList(0, 10));

                // 각 클러스터 중심점 개선 (k개)
                List<KmeanSet> centroids = new ArrayList<>();
                for (List<KmeanSet> cluster : clusters) {
//                    System.out.println("cluster: " + cluster.get(0));
                    double sumX = 0;
                    double sumY = 0;
                    for (KmeanSet point : cluster) {
                        sumX += point.getX();
                        sumY += point.getY();
                    }
                    double centroidX = sumX / cluster.size();
                    double centroidY = sumY / cluster.size();
                    centroids.add(new KmeanSet(centroidX, centroidY));
                }

//                System.out.println("len - cluster (1)" + clusters.get(0));
//                System.out.println("len - cluster (2)" + clusters.get(1));
//                System.out.println("len - cluster (3)" + clusters.get(2));
//                System.out.println("centeroid : "+ centroids.get(0));
//                System.out.println("centeroid : "+ centroids.get(1));
//                System.out.println("centeroid : "+ centroids.get(2));

                // 결과 출력
                // k개의 중심을 각 클러스터와 매핑하여 write 하기
                for (int i = 0; i < k; i++) {
                    for (KmeanSet cluster : clusters.get(i)) {
                        context.write(new Text(centroids.get(i).toString()),
                                new Text(cluster.toString()));
                    }
                }
            }
            else if (len != 0) {
                // 특정 centroid 값으로 묶인 클러스터 좌표 값의 평균 계산
                double sumX = 0;
                double sumY = 0;
                for (KmeanSet xy : xYs) {
                    sumX += xy.getX();
                    sumY += xy.getY();
                }
                double newCentroidX = sumX / xYs.size();
                double newCentroidY = sumY / xYs.size();
                KmeanSet newCentroid = new KmeanSet(newCentroidX, newCentroidY);

                // 기존 중심점과 비교
                if (newCentroid.getX() == key.getX() && newCentroid.getY() == key.getY()) {
                    System.out.println("Centroid has not changed: " + newCentroid);
                    throw new InterruptedException("중심이 변하지 않았습니다.");
                }

                System.out.println("중심 이동");
                for (KmeanSet xy : xYs) {
                    context.write(new Text(newCentroid.toString()), new Text(xy.toString()));
                }
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        System.out.println("Running k-means Clustering");

        k = Integer.parseInt(args[2]);
        int iter = 1;

        System.out.println("k - means {0}");

        Job Kmeans_1 = Job.getInstance(getConf());
        Kmeans_1.setJobName("k - means {0}");

        Kmeans_1.setJarByClass(KmeansClustering.class);
        Kmeans_1.setMapperClass(KmeansMapFirst.class);
        Kmeans_1.setReducerClass(KmeansReduce.class);

        Kmeans_1.setMapOutputKeyClass(KmeanSet.class);
        Kmeans_1.setMapOutputValueClass(KmeanSet.class);
        Kmeans_1.setOutputKeyClass(KmeanSet.class);
        Kmeans_1.setOutputValueClass(KmeanSet.class);

        Kmeans_1.setOutputFormatClass(TextOutputFormat.class);
        Kmeans_1.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(Kmeans_1, new Path(args[0] + "/dense_coordinates.txt"));
        FileOutputFormat.setOutputPath(Kmeans_1, new Path(args[1] + "/" + iter));

        Kmeans_1.waitForCompletion(true);

        // job 2
        while(true) {
            System.out.println("k - means {" + iter + "}");

            Job Kmeans = Job.getInstance(getConf());
            Kmeans.setJobName("k - means {" + iter + "}");

            Kmeans.setJarByClass(KmeansClustering.class);
            Kmeans.setMapperClass(KmeansMap.class);
            Kmeans.setReducerClass(KmeansReduce.class);

            Kmeans.setMapOutputKeyClass(KmeanSet.class);
            Kmeans.setMapOutputValueClass(KmeanSet.class);
            Kmeans.setOutputKeyClass(KmeanSet.class);
            Kmeans.setOutputValueClass(KmeanSet.class);

            Kmeans.setOutputFormatClass(TextOutputFormat.class);
            Kmeans.setInputFormatClass(TextInputFormat.class);

            Path kPath = new Path(args[1] + "/" + iter + "/part-r-00000");
            Kmeans.addCacheFile(kPath.toUri());
            FileInputFormat.addInputPath(Kmeans, new Path(args[0] + "/dense_coordinates.txt"));
            FileOutputFormat.setOutputPath(Kmeans, new Path(args[1] + "/" + (iter + 1)));

            if (!Kmeans.waitForCompletion(true)) {
                System.out.println("중심을 찾아 종료합니당");
                break;
            }
            iter += 1;
        }

        return 0;
    }
}