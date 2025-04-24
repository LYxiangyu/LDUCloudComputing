package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class UserBehaviorAnalysis {

    public static void main(String[] args) throws IOException {
        // 1. 创建 SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("User Behavior Analysis")
                .master("local[*]")
                .getOrCreate();

        // 2. 设置 HDFS 配置
        String hdfsUri = "hdfs://master:9000"; // HDFS URI
        String hdfsPath = "/user/hadoop/user_behavior_logs.csv"; // HDFS 路径

        // 3. 随机生成用户行为日志数据
        List<String> logData = generateRandomUserBehaviorLogs(150);

        // 4. 将数据写入 HDFS
        writeToHDFS(spark, hdfsUri, hdfsPath, logData);

        // 5. 读取日志数据
        Dataset<Row> userLogs = spark.read().csv(hdfsPath);

        // 6. 转换日志数据为合适的格式
        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.StringType, false),
                DataTypes.createStructField("item_id", DataTypes.StringType, false),
                DataTypes.createStructField("category_id", DataTypes.StringType, false),
                DataTypes.createStructField("behavior_type", DataTypes.StringType, false),
                DataTypes.createStructField("timestamp", DataTypes.StringType, false)
        ));

        userLogs = spark.read().schema(schema).csv(hdfsPath);

        // 7. 分析：统计每个用户的浏览(pv)次数
        Dataset<Row> pvCounts = userLogs.filter("behavior_type = 'pv'")
                .groupBy("user_id").count();

        // 8. 分析：找出最受欢迎的10个商品
        Dataset<Row> top10Items = userLogs.groupBy("item_id")
                .agg(functions.count("*").alias("count"))
                .orderBy(functions.desc("count"))
                .limit(10);

        // 9. 分析：计算购买转化率
        long totalActions = userLogs.count();
        long buyActions = userLogs.filter("behavior_type = 'buy'").count();
        double conversionRate = (double) buyActions / totalActions;

        // 10. 输出结果
        System.out.println("每个用户的浏览(pv)次数:");
        pvCounts.show();

        System.out.println("最受欢迎的10个商品:");
        top10Items.show();

        System.out.println("购买转化率: " + conversionRate);

        // 关闭 SparkSession
        spark.stop();
    }

    // 随机生成用户行为日志
    private static List<String> generateRandomUserBehaviorLogs(int numLogs) {
        Random random = new Random();
        String[] behaviorTypes = {"pv", "cart", "fav", "buy"};
        String[] itemIds = {"2001", "2002", "2003", "2004", "2005"};
        String[] categoryIds = {"3001", "3002", "3003", "3004", "3005"};
        String[] userIds = {"1001", "1002", "1003", "1004", "1005"};

        return random.ints(numLogs, 0, 100)
                .mapToObj(i -> {
                    String userId = userIds[random.nextInt(userIds.length)];
                    String itemId = itemIds[random.nextInt(itemIds.length)];
                    String categoryId = categoryIds[random.nextInt(categoryIds.length)];
                    String behaviorType = behaviorTypes[random.nextInt(behaviorTypes.length)];
                    String timestamp = "2023-01-01 " + (random.nextInt(24) + 1) + ":" + (random.nextInt(60)) + ":" + (random.nextInt(60));
                    return userId + "," + itemId + "," + categoryId + "," + behaviorType + "," + timestamp;
                })
                .collect(Collectors.toList());
    }

    // 将日志数据写入 HDFS
    private static void writeToHDFS(SparkSession spark, String hdfsUri, String hdfsPath, List<String> data) throws IOException {
        Configuration hadoopConf = new Configuration();
        FileSystem fs = FileSystem.get(hadoopConf);
        Path path = new Path(hdfsUri + hdfsPath);

        if (fs.exists(path)) {
            fs.delete(path, true);  // 如果文件已存在，则删除
        }

        // 使用 JavaRDD 的 parallelize 方法
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> rdd = sc.parallelize(data); // 这里不需要指定分区数，默认即可
        rdd.saveAsTextFile(hdfsUri + hdfsPath); // 保存数据到 HDFS
    }
}
