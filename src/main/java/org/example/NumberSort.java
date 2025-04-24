package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class NumberSort {

    private static final String HDFS_URI = "hdfs://20.33.32.192:9000";
    private static final String HDFS_INPUT_PATH = "/input/numbers.txt";
    // 生成随机数文件
    public static void generateRandomNumbersInHDFS(int count) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);
        FileSystem fs = FileSystem.get(URI.create(HDFS_URI), conf);

        // 创建 HDFS 文件
        Path filePath = new Path(HDFS_INPUT_PATH);
        if (fs.exists(filePath)) {
            fs.delete(filePath, true);
        }

        OutputStream os = fs.create(filePath);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os));

        Random random = new Random();
        for (int i = 0; i < count; i++) {
            writer.write(random.nextInt(10000) + "\n");  // 生成 0-9999 的随机数
        }

        writer.close();
        fs.close();
        System.out.println("已直接生成随机数文件于 HDFS：" + HDFS_INPUT_PATH);
    }


    public static class SortMapper extends Mapper<Object, Text, IntWritable, NullWritable> {
        private final IntWritable number = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                int parsed = Integer.parseInt(value.toString().trim());
                number.set(parsed);
                System.out.println("Mapper 输入: " + value + " 输出: <" + parsed + ", null>");
                context.write(number, NullWritable.get());
            } catch (NumberFormatException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public static class SortReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {
        private List<Integer> numberList = new ArrayList<>();

        public void reduce(IntWritable key, Iterable<NullWritable> values, Context context) {
            numberList.add(key.get());
            System.out.println("Reducer 收到 key: " + key.get());
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 在 Reducer 端进行冒泡排序
            int n = numberList.size();
            for (int i = 0; i < n - 1; i++) {
                for (int j = 0; j < n - i - 1; j++) {
                    if (numberList.get(j) > numberList.get(j + 1)) {
                        // 交换
                        int temp = numberList.get(j);
                        numberList.set(j, numberList.get(j + 1));
                        numberList.set(j + 1, temp);
                    }
                }
            }

            // 输出排序后的数据
            for (int num : numberList) {
                context.write(new IntWritable(num), NullWritable.get());
            }
        }
    }

    public static class SortCombiner extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {
        public void reduce(IntWritable key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            System.out.println("Combiner 合并 key: " + key.get());
            context.write(key, NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {
        String hdfsOutputPath = "/output332/sorted";

        // 直接在 HDFS 生成随机数文件
        generateRandomNumbersInHDFS(1000);

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);

        Job job = Job.getInstance(conf, "Number Sort");
        job.setJarByClass(NumberSort.class);
        job.setMapperClass(SortMapper.class);
        job.setCombinerClass(SortCombiner.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(HDFS_INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
