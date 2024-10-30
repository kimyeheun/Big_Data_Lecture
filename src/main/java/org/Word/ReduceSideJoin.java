package org.Word;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class ReduceSideJoin extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ReduceSideJoin(), args);
    }

    // JOB 1
    public static class MapperLineItem extends Mapper<Object, Text, Text, IntWritable> {
        protected void map(Object l, Text L)
            throws IOException, InterruptedException {

            for (String record : L.toString().split(",")) {
                System.out.println(record);
            }
            // L: a recode of lineitem table

            // d <- 'DATA'
            // key <- partkey + "|" + suppkey
            // value <- quantity

            // if (shipdata >= d and shipdata < d + 1 year): emit(key, value)
        }
    }

    public static class ReducerLineItem extends Reducer<Text, IntWritable, Text, IntWritable> {

    }

    /**
     * Driver
     * @param args
     * @return 0
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        // Job 생성
        Job reduceSide = Job.getInstance(getConf());
        // Job 이름(ID) 생성
        reduceSide.setJobName("ReduceSideJoin");
        // key 설정
        reduceSide.setJarByClass(ReduceSideJoin.class);
        reduceSide.setMapperClass(MapperLineItem.class);
        reduceSide.setReducerClass(ReducerLineItem.class);
        // OUTPUT <key, value> 세팅
        reduceSide.setMapOutputKeyClass(Text.class);
        reduceSide.setMapOutputValueClass(DoubleWritable.class);
        // input, output format 넣기
        reduceSide.setInputFormatClass(TextInputFormat.class);
        reduceSide.setOutputFormatClass(TextOutputFormat.class);
        // input, output 경로 설정
        FileInputFormat.addInputPath(reduceSide, new Path(args[0]));
        FileInputFormat.addInputPath(reduceSide, new Path(args[1]));
        // job 실행
        reduceSide.waitForCompletion(true);
        return 0;
    }
}
