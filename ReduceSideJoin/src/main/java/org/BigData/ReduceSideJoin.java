package org.BigData;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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

            // key <- partkey + "|" + suppkey + "|q" <- 구분자 삽입!
            key.set(buffer[1]+"|"+buffer[2] +  "|q"); // L_PARTKEY|L_SUPPKEY
            // value <- quantity
            value.set(Integer.parseInt(buffer[4]));

            // if (shipdate >= d and shipdate < d + 1 year): emit(key, value)
            LocalDate shipdate = LocalDate.parse(buffer[10]);

            if ((d.isBefore(shipdate) || d.isEqual(shipdate))
                    && d_1.isAfter(shipdate)) {
                context.write(key, value);
            }
        }
    }


    /**
     * 이름: JOB 2 - MapperPartSupp
     * 데이터 라인을 불러와서 hadoop에 <key, value> 형태로 저장
     * L (출력 예시): 5|2506|9653|50.52|y stealthy deposits. furiously final pinto beans wake furiou|
     *
     * 주의 : Reducer에서 해당 v 값을 MapperLineItem와 구분할 수 있어야 한다.
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

            // key <- partkey + "|" + suppkey + "|a" <- 구분자 삽입!
            key.set(buffer[0] + "|" + buffer[1] + "|a"); // PS_PARTKEY|PS_SUPPKEY
            // value <- availqty
            value.set(Integer.parseInt(buffer[2]));
            // EMIT(k, v)
            context.write(key, value);
        }
    }

    /**
     * 이름: JOB 3 - MapperForReducer
     * 데이터 라인을 불러와서 hadoop에 key 값의 타입 (q, a)를 읽고, value에 타입을 적용하여 Reducer로 넘김
     *
     * input : 100000|2510|q	31
     * return : 100000|2510	31q
     */
    public static class MapperForReducer extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String sep = key.toString().endsWith("a") ? "a" : "q";
            String newKey = key.toString().substring(0, key.toString().length() - 2);
            Text newValue = new Text(value + sep);

            context.write(new Text(newKey), newValue);
        }
    }

    /**
     * 이름: JOB 3 - ReducerSide
     * <key, value> 를 불러와서 join
     *
     * [1]. 데이터 읽기
     * 100000|2510	31q
     * 100000|2510	433a 형태의 값들을 읽고, 리스트로 저장.
     * [2]. key 값 별로 value 묶기
     * 100000|2510	31q 433a
     * [3]. quantity의 합계 구하기 + availqty 찾기
     * [4]. 조건을 만족하는지 확인
     *
     * return : 100000 2510 (특정 조건을 만족한 두 개의 key 값)
     */
    public static class ReducerSide extends
            Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            int availqty = -1;

            List<String> valueList = new ArrayList<>();
            for (Text val : values) {
                valueList.add(val.toString());
            }

            for (String val : valueList) {
                int intval = Integer.parseInt(val.substring(0, val.length() - 1));
                if (val.endsWith("q")) {
                    // sum ← sum + quantity
                    sum += intval;
                }
                else { // (key.endsWith("a"))
                    availqty = intval;
                }
            }

            // if (availqty exist in valuse [v1, v2, …]) <- values는 item 테이블과 supp 테이블의 값들로 이루어져 있다.
            if (availqty != -1) {
                // if (availqty > 0.5 * sum )
                if (sum > 0 && availqty > 0.5 * sum) {
                    // s[] ← split (k, “|” )
                    String[] realKeys = key.toString().split("\\|");
                    // EMIT (s[0], s[1] )
                    Text s0 = new Text(realKeys[0]);
                    Text s1 = new Text(realKeys[1]);
                    context.write(s0, s1);
                }
            }
        }
    }


    public int run(String[] args) throws Exception {
        System.out.println("Running ReduceSideJoin");

        // NOTE: MAP
        // Job1 생성
        Job maplineitem = Job.getInstance(getConf());
        // Job 이름(ID) 생성
        maplineitem.setJobName("MapperLineItem");
        // Mapper, Reducer 설정
        maplineitem.setJarByClass(ReduceSideJoin.class);
        maplineitem.setMapperClass(MapperLineItem.class);
        // OUTPUT <key, value> 세팅
        maplineitem.setMapOutputKeyClass(Text.class);
        maplineitem.setMapOutputValueClass(IntWritable.class);
        // input, output format 넣기
        maplineitem.setInputFormatClass(TextInputFormat.class);
        maplineitem.setOutputFormatClass(TextOutputFormat.class);
        // input, output 경로 설정
        FileInputFormat.addInputPath(maplineitem, new Path(args[0]+"/lineitem.tbl"));
        FileOutputFormat.setOutputPath(maplineitem, new Path(args[1]+"/line"));
        // job 실행
        maplineitem.waitForCompletion(true);

        // Job2 생성
        Job mappartsupp = Job.getInstance(getConf());
        // Job 이름(ID) 생성
        mappartsupp.setJobName("MapperPartSupp");
        // Mapper, Reducer 설정
        mappartsupp.setJarByClass(ReduceSideJoin.class);
        mappartsupp.setMapperClass(MapperPartSupp.class);
        // OUTPUT <key, value> 세팅
        mappartsupp.setMapOutputKeyClass(Text.class);
        mappartsupp.setMapOutputValueClass(IntWritable.class);
        // input, output format 넣기
        mappartsupp.setInputFormatClass(TextInputFormat.class);
        mappartsupp.setOutputFormatClass(TextOutputFormat.class);
        // input, output 경로 설정
        FileInputFormat.addInputPath(mappartsupp, new Path(args[0]+"/partsupp.tbl"));
        FileOutputFormat.setOutputPath(mappartsupp, new Path(args[1]+"/part"));
        // job 실행
        mappartsupp.waitForCompletion(true);

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
        reduceSide.setMapOutputValueClass(Text.class);
        // input, output format 넣기
        reduceSide.setInputFormatClass(KeyValueTextInputFormat.class);
        reduceSide.setOutputFormatClass(TextOutputFormat.class);
        // input, output 경로 설정
        FileInputFormat.addInputPath(reduceSide, new Path(args[1]+"/line/part-r-00000"));
        FileInputFormat.addInputPath(reduceSide, new Path(args[1]+"/part/part-r-00000"));
        FileOutputFormat.setOutputPath(reduceSide, new Path(args[1]+"/final"));

        // job 실행
        reduceSide.waitForCompletion(true);
        return 0;
    }
}
