package org.BigData;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;


public class FrequentItemsetMining extends Configured implements Tool {
    public static int minsupport;
    public static int k;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new FrequentItemsetMining(), args);
    }

    /**
     * 이름: ApriMap
     * 데이터 라인을 불러와서 빈도를 value로 하도록 <key, value> 출력
     *
     * input : 25 52 164 240 274 328 368 448 538 561 630 687 730 775 825 834
     * return : 25 1
     *          52 1
     *          ...
     */
    public static class ApriMap extends Mapper<Object, Text, Text, IntWritable> {
        Text word = new Text();
        IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            StringTokenizer st = new StringTokenizer(value.toString());
            while (st.hasMoreTokens()) {
                word.set(st.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class ApriKMap extends Mapper<Object, Text, Text, IntWritable> {
        Set<String> kItems = new HashSet<>();
        Text word = new Text();
        IntWritable one = new IntWritable(1);

        /**
         * 이전 reducer output 가져와서 key 값 리스트로 반환 (빈 파일이면 오류 -> Driver에서 처리 == 종료)
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
                    System.out.println("pair : "+pair[0]);
                    String keyPart = pair[0]; // key 부분
                    String[] keys = keyPart.split("\\.");
                    for (String key : keys) {
                        kItems.add(key.trim());
                    }
                }
                reader.close();
//                System.out.println("Loaded kItems: " + kItems);
            }

            if (kItems.isEmpty()) {
                System.err.println("kItems is empty. Exiting Mapper.");
                throw new InterruptedException("No data to process in kItems.");
            }
        }


        public static List<List<String>> generateCombinations(Set<String> keyList) {
            List<String> keys = new ArrayList<>(keyList);  // Set을 List로 변환

            List<List<String>> result = new ArrayList<>();
            List<String> combination = new ArrayList<>();
            backtrack(keys, 0, combination, result);

            return result;  // 모든 조합을 반환
        }

        private static void backtrack(List<String> keys, int start, List<String> combination, List<List<String>> result) {
            if (combination.size() == k) {
                result.add(new ArrayList<>(combination));
                return;
            }

            for (int i = start; i < keys.size(); i++) {
                combination.add(keys.get(i));
                backtrack(keys, i + 1, combination, result);
                combination.remove(combination.size() - 1);
            }
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            // 원본 문서에서 각 줄을 읽어, 각 key 쌍을 가지고 있는 빈도를 구하기
            String[] lines = value.toString().split("\\s+");

            // kItems의 조합 구하기
            List<List<String>> combineKeyList = generateCombinations(kItems);

            // lines를 보면서, combineKeyList의 요소가 모두! 포함되어있는지 확인
            for (List<String> combineKey: combineKeyList){
                boolean includeAllKeys = true;

                // 각 키가 linesSet에 포함되어 있는지 확인
                for (String eachKey : combineKey) {
//                    System.out.println("eachKey : " + eachKey);
                    if (!Arrays.asList(lines).contains(eachKey)) {
                        includeAllKeys = false;
                        break;
                    }
                }

                // 모든 키가 포함되어 있으면 새로운 키 생성 및 출력
                if (includeAllKeys) {
                    String newKey = String.join(".", combineKey);
                    word.set(newKey);
                    context.write(word, one);
                }
            }
        }
    }
    public static class ApriReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable freq = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            if(minsupport <= sum) {
                freq.set(sum);
                context.write(key, freq);
            }
        }
    }

    public int run(String[] args) throws Exception {
        System.out.println("Running Apriori");

        minsupport = Integer.parseInt(args[2]);
        k = 1;

        // job 1: apriori k = 1
        Job Apriori_1 = Job.getInstance(getConf());
        Apriori_1.setJobName("Apriori-1");

        Apriori_1.setJarByClass(FrequentItemsetMining.class);
        Apriori_1.setMapperClass(ApriMap.class);
        Apriori_1.setReducerClass(ApriReduce.class);

        Apriori_1.setMapOutputKeyClass(Text.class);
        Apriori_1.setMapOutputValueClass(IntWritable.class);

        Apriori_1.setOutputFormatClass(TextOutputFormat.class);
        Apriori_1.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(Apriori_1, new Path(args[0] + "/T10I4D100K.dat"));
        FileOutputFormat.setOutputPath(Apriori_1, new Path(args[1] + k));

        Apriori_1.waitForCompletion(true);

        // job 2: apriori k > 1
        while(true) {
            System.out.println("Running Apriori - " + (k + 1));

            Job Apriori_k = Job.getInstance(getConf());
            Apriori_k.setJobName("Apriori-" + (k + 1));

            Apriori_k.setJarByClass(FrequentItemsetMining.class);
            Apriori_k.setMapperClass(ApriKMap.class);
            Apriori_k.setReducerClass(ApriReduce.class);

            Apriori_k.setMapOutputKeyClass(Text.class);
            Apriori_k.setMapOutputValueClass(IntWritable.class);

            Apriori_k.setOutputFormatClass(TextOutputFormat.class);
            Apriori_k.setInputFormatClass(TextInputFormat.class);

            Path kPath = new Path(args[1] + k + "/part-r-00000");
            FileSystem fs = FileSystem.get(getConf());
            if (!fs.exists(kPath)) {
                System.out.println("파일이 존재하지 않습니다: " + kPath);
                break; // 반복 종료
            }
            Apriori_k.addCacheFile(kPath.toUri());
            FileInputFormat.addInputPath(Apriori_k, new Path(args[0] + "/T10I4D100K.dat"));

            k++;
            FileOutputFormat.setOutputPath(Apriori_k, new Path(args[1] + k));

            if (!Apriori_k.waitForCompletion(true)) {
                System.out.println("Apriori로 만들어낼 수 있는 항목이 더 이상 없습니당");
                break;
            }
        }
        return 0;
    }

}

