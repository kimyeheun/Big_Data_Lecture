package org.BigData;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.time.LocalDate;
import java.util.Random;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceSideJoin extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ReduceSideJoin(), args);
    }

    private static LocalDate getRandomDate() {
        Random random = new Random();
        int year = 1993 + random.nextInt(5); // 1993부터 1997까지의 무작위 연도
        return LocalDate.of(year, 1, 1);
    }

    // JOB 1
    /**
     * 이름: JOB 1 - MapperLineItem
     * 데이터 라인을 불러와서 hadoop에 <key, value> 형태로 저장
     * L (출력 예시): 3|29380|1883|4|2|2618.76|0.01|0.06|A|F|1993-12-04|1994-01-07|1994-01-01|NONE|TRUCK|y. fluffily pending d|
     *              L_ORDERKEY|L_PARTKEY|
     * 조건: shipdate
     *
     * return : 100000|2510	9282  == partkey|suppkey  quantity(주문된 부품의 개수)
     */
    public static class MapperLineItem extends Mapper<Object, Text, Text, IntWritable> {
        Text key = new Text();
        IntWritable value = new IntWritable();

        @Override
        protected void map(Object l, Text L,
                           Mapper<Object, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {

            // L: a recode of lineitem table
            String[] buffer = L.toString().split("\\|");

            // d <- 'DATA'
            LocalDate d = getRandomDate(); // 1993~1997년 사이의 무작위 연도의 1월 1일
            LocalDate d_1 = d.plusYears(1);

            // key <- partkey + "|" + suppkey
            key.set(buffer[1]+"|"+buffer[2]); // L_PARTKEY|L_SUPPKEY
            // value <- quantity
            value.set(Integer.parseInt(buffer[4]));

            // if (shipdate >= d and shipdate < d + 1 year): emit(key, value)
            LocalDate shipdate = LocalDate.parse(buffer[10]);

            if ((d.isBefore(shipdate) || d.isEqual(shipdate))
                    && d_1.isAfter(shipdate)) {
                System.out.println("Mapper Key 타입 : " + key.toString());
                System.out.println("Mapper Value 타입: " + value.toString());
                context.write(key, value);
            }
        }
    }

    /**
     * 이름: JOB 2 - MapperPartSupp
     * 데이터 라인을 불러와서 hadoop에 <key, value> 형태로 저장
     * L (출력 예시): 5|2506|9653|50.52|y stealthy deposits. furiously final pinto beans wake furiou|
     *
     * return : 100000|2510	31 == partkey|suppkey  availqty(제공 가능한 부품의 수량)
     */
    public static class MapperPartSupp extends Mapper<Object, Text, Text, IntWritable> {
        Text key = new Text();
        IntWritable value = new IntWritable();

        @Override
        protected void map(Object l, Text L,
                           Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            // L: a recode of partsupp table
            String[] buffer = L.toString().split("\\|");

            // key <- partkey + "|" + suppkey
            key.set(buffer[0] + "|" + buffer[1]); // PS_PARTKEY|PS_SUPPKEY
            // value <- availqty
            value.set(Integer.parseInt(buffer[2]));
            // EMIT(k, v)
            System.out.println("Mapper Key 타입 : " + key.toString());
            System.out.println("Mapper Value 타입: " + value.toString());
            context.write(key, value);
        }
    }

//    public static class ReducerSide extends
//            Reducer<Text, IntWritable, Text, IntWritable> {
//
//        @Override
//        protected void reduce(Text key, Iterable<IntWritable> values,
//                              Context context)
//            throws IOException, InterruptedException {
//
//            // FIXME: values 출력 어떻게 나오는지 확인.
//            System.out.println("Reducer key : " + key.toString());
//
//            int sum = 0;
//
//            for (IntWritable val : values) {
//                sum = sum + val.get();  // val.get() = quantity
//            }
//
//            context.write(key, new IntWritable(sum));
//        }
//    }

    public static class MapperForReducer extends Mapper<Text, Text, Text, IntWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int intValue = Integer.parseInt(value.toString());
            context.write(key, new IntWritable(intValue));
        }
    }

    public static class ReducerSide extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // 값을 합산하는 부분
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();  // 각 Mapper에서 전달된 수량을 더함
            }

            // 최종 결과를 출력
            context.write(key, new IntWritable(sum));
        }
    }

    /**
     * Driver
     * @param args
     * @return 0
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        System.out.println("Running ReduceSideJoin");

//        // NOTE: MAP
//        // Job1 생성
//        Job maplineitem = Job.getInstance(getConf());
//        // Job 이름(ID) 생성
//        maplineitem.setJobName("MapperLineItem");
//        // Mapper, Reducer 설정
//        maplineitem.setJarByClass(ReduceSideJoin.class);
//        maplineitem.setMapperClass(MapperLineItem.class);
//        // OUTPUT <key, value> 세팅
//        maplineitem.setMapOutputKeyClass(Text.class);
//        maplineitem.setMapOutputValueClass(IntWritable.class);
//        // input, output format 넣기
//        maplineitem.setInputFormatClass(TextInputFormat.class);
//        maplineitem.setOutputFormatClass(TextOutputFormat.class);
//        // input, output 경로 설정
//        FileInputFormat.addInputPath(maplineitem, new Path(args[0]+"/lineitem.tbl"));
//        FileOutputFormat.setOutputPath(maplineitem, new Path(args[1]+"/line"));
//        // job 실행
//        maplineitem.waitForCompletion(true);
//
//        // Job2 생성
//        Job mappartsupp = Job.getInstance(getConf());
//        // Job 이름(ID) 생성
//        mappartsupp.setJobName("MapperPartSupp");
//        // Mapper, Reducer 설정
//        mappartsupp.setJarByClass(ReduceSideJoin.class);
//        mappartsupp.setMapperClass(MapperPartSupp.class);
//        // OUTPUT <key, value> 세팅
//        mappartsupp.setMapOutputKeyClass(Text.class);
//        mappartsupp.setMapOutputValueClass(IntWritable.class);
//        // input, output format 넣기
//        mappartsupp.setInputFormatClass(TextInputFormat.class);
//        mappartsupp.setOutputFormatClass(TextOutputFormat.class);
//        // input, output 경로 설정
//        FileInputFormat.addInputPath(mappartsupp, new Path(args[0]+"/partsupp.tbl"));
//        FileOutputFormat.setOutputPath(mappartsupp, new Path(args[1]+"/part"));
//        // job 실행
//        mappartsupp.waitForCompletion(true);

        // TODO: reducer 결과 출력하기
        System.out.println("Running Reducer");
        // Job3 생성
        Job reduceSide = Job.getInstance(getConf());
        // Job 이름(ID) 생성
        reduceSide.setJobName("Reducer");
        // Mapper, Reducer 설정
        reduceSide.setJarByClass(ReduceSideJoin.class);

        reduceSide.setMapperClass(MapperForReducer.class);
        reduceSide.setReducerClass(ReducerSide.class);
        // OUTPUT <key, value> 세팅
        reduceSide.setMapOutputKeyClass(Text.class);
        reduceSide.setMapOutputValueClass(IntWritable.class);
        // input, output format 넣기
        reduceSide.setInputFormatClass(KeyValueTextInputFormat.class);
        reduceSide.setOutputFormatClass(TextOutputFormat.class);
        // input, output 경로 설정
        System.out.println("Running Reducer?");

        FileInputFormat.addInputPath(reduceSide, new Path(args[1]+"/line/part-r-00000"));
        FileInputFormat.addInputPath(reduceSide, new Path(args[1]+"/part/part-r-00000"));
        FileOutputFormat.setOutputPath(reduceSide, new Path(args[1]+"/final"));

        // job 실행
        reduceSide.waitForCompletion(true);
        return 0;
    }
}
