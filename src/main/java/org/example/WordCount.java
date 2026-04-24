package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class WordCount {

    // HDFS 配置（请根据实际情况修改）
    private static final String HDFS_URI = "hdfs://21.0.98.130:9000";
    private static final String INPUT_PATH = "/input";   // 目录下包含 words1.txt, words2.txt, words3.txt
    private static final String OUTPUT_PATH = "/output/wordcount";

    // Mapper: 读取单词，输出 (word, 1)
    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();   // 统一转为小写
            StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r\f,.:;?!\"'()[]{}<>");
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                // 只保留字母开头的单词（可选）
                if (token.length() > 0 && Character.isLetter(token.charAt(0))) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }

    // 自定义分区器：根据单词首字母分配到不同 Reducer
    public static class WordPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String word = key.toString().toLowerCase();
            if (word.isEmpty()) return 0;
            char firstChar = word.charAt(0);
            if (firstChar >= 'a' && firstChar <= 'k') return 0;
            if (firstChar >= 'l' && firstChar <= 'r') return 1;
            return 2;   // s-z 及其他字符
        }
    }

    // Reducer: 统计单词总数，并在 cleanup 中按次数降序输出
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
        private List<WordCountPair> wordCountList = new ArrayList<>();

        // 保存 (单词, 总次数)
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            wordCountList.add(new WordCountPair(key.toString(), sum));
        }

        // 在 Reducer 结束前对本分区的所有单词按次数降序排序并输出
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 按次数降序排序，次数相同时按字母升序
            wordCountList.sort((a, b) -> {
                if (a.count != b.count) return Integer.compare(b.count, a.count);
                return a.word.compareTo(b.word);
            });

            // 输出格式：count######word
            for (WordCountPair pair : wordCountList) {
                String outputLine = pair.count + "######" + pair.word;
                context.write(new Text(outputLine), NullWritable.get());
            }
        }

        // 辅助内部类
        static class WordCountPair {
            String word;
            int count;
            WordCountPair(String word, int count) {
                this.word = word;
                this.count = count;
            }
        }
    }

    // 可选 Combiner（直接复用 Reducer 的逻辑，但注意 Reducer 中使用了 List，Combiner 不应使用 cleanup）
    // 为避免内存问题，Combiner 直接累加并输出，不排序
    public static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);

        Job job = Job.getInstance(conf, "Word Count with Partition and Sort");
        job.setJarByClass(WordCount.class);

        // 设置 Mapper、Combiner、Partitioner、Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountCombiner.class);
        job.setPartitionerClass(WordPartitioner.class);
        job.setReducerClass(WordCountReducer.class);

        // Reducer 数量必须与分区数一致（3个区）
        job.setNumReduceTasks(3);

        // Mapper 输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reducer 最终输出类型（Text 作为整行，NullWritable 无值）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}