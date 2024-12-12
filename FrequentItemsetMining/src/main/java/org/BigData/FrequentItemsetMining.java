package org.BigData;

import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapreduce.task.reduce.Shuffle;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


public class FrequentItemsetMining extends Configured implements Tool {
    public static int minsupport;

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

    public static class ApriKMap extends Mapper<Text, Text, Text, IntWritable> {
        Text word = new Text();
        IntWritable one = new IntWritable(1);

        @Override
        protected void map(Text key, Text value,
                           Mapper<Text, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            List<String> keyList = new ArrayList<>();
            // 조합으로 key 집합 구하기

            // 각 key 쌍을 가지고 있는 빈도를 구하기
            // for key in keys
            // contains
            StringTokenizer st = new StringTokenizer(value.toString());
            while (st.hasMoreTokens()) {
                word.set(st.nextToken());
                context.write(word, one);
            }
        }
    }

//    public static class ApriShuffle extends Shuffle<Object, Text, Text, IntWritable> {
//
//        @Override
//        protected void (Object key, Text value,
//                           Mapper<Object, Text, Text, IntWritable>.Context context)
//                throws IOException, InterruptedException {
//            StringTokenizer st = new StringTokenizer(value.toString());
//            while (st.hasMoreTokens()) {
//                word.set(st.nextToken());
//                context.write(word, one);
//            }
//        }
//    }

    public static class ApriReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable oval = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            if(minsupport <= sum) {
                oval.set(sum);
                context.write(key, oval);
            }
        }
    }

    public int run(String[] args) throws Exception {
        System.out.println("Running Apriori");

        minsupport = Integer.parseInt(args[2]);

        // job 1: apriori k = 1
        Job Apriori_1 = Job.getInstance(getConf());

        Apriori_1.setJarByClass(FrequentItemsetMining.class);
        Apriori_1.setMapperClass(ApriMap.class);
        Apriori_1.setReducerClass(ApriReduce.class);

        Apriori_1.setMapOutputKeyClass(Text.class);
        Apriori_1.setMapOutputValueClass(IntWritable.class);

        Apriori_1.setOutputFormatClass(TextOutputFormat.class);
        Apriori_1.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(Apriori_1, new Path(args[0]));
        FileOutputFormat.setOutputPath(Apriori_1, new Path(args[1]));

        Apriori_1.waitForCompletion(true);

        // job 2: apriori k <-
        return 0;
    }

}

