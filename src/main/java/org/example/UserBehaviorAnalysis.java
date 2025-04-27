package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class UserBehaviorAnalysis {

    public static void main(String[] args) throws IOException {
        // 初始化Spark配置和Context
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "hdfs://20.33.32.192:9000"); // 修改为你的HDFS URI
        FileSystem fs = FileSystem.get(hadoopConf);

        // 初始化SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("UserBehaviorAnalysis")
                .master("local")  // 使用本地模式运行
                .getOrCreate();

        // 创建列名数组
        String[] columnNames = {"user_id", "item_id", "category_id", "behavior_type", "timestamp"};

        // 定义行为类型
        String[] behaviorTypes = {"pv", "cart", "fav", "buy"};

        // 生成150条随机行为日志
        List<String> logData = generateLogData(150, behaviorTypes);
        // === 新增：将生成的数据写入HDFS ===
        Path outputPath = new Path("/user/hadoop/user_behavior.log");
        // 如果HDFS上已经有这个文件，先删掉（避免冲突）
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // 写入HDFS
        FSDataOutputStream outputStream = fs.create(outputPath);
        for (String line : logData) {
            outputStream.writeBytes(line + "\n"); // 一行一条记录
        }
        outputStream.close();
        System.out.println("日志数据已成功写入到 HDFS: " + outputPath.toString());


        // 将数据转为RDD
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        // 从HDFS读取数据
        JavaRDD<String> logRDD = sc.textFile("hdfs://20.33.32.192:9000/user/hadoop/user_behavior.log");


        // 构建Schema
        StructField[] fields = new StructField[]{
                DataTypes.createStructField("user_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("item_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("category_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("behavior_type", DataTypes.StringType, false),
                DataTypes.createStructField("timestamp", DataTypes.StringType, false)
        };
        StructType schema = DataTypes.createStructType(fields);

        // 将RDD转换为DataFrame
        JavaRDD<Row> rowRDD = logRDD.map(line -> {
            String[] fieldsArray = line.split(",");
            return RowFactory.create(Integer.parseInt(fieldsArray[0]), Integer.parseInt(fieldsArray[1]), Integer.parseInt(fieldsArray[2]), fieldsArray[3], fieldsArray[4]);
        });

        Dataset<Row> logDF = spark.createDataFrame(rowRDD, schema);

        // 分析1: 统计每个用户的浏览次数(pv)
        Dataset<Row> pvCountDF = logDF.filter("behavior_type = 'pv'")
                .groupBy("user_id")
                .count()
                .withColumnRenamed("count", "pv_count");
        pvCountDF.show();

        // 分析2: 找出最受欢迎的10个商品
        Dataset<Row> popularItemsDF = logDF.groupBy("item_id")
                .count()
                .orderBy(functions.desc("count"))
                .limit(10);
        popularItemsDF.show();

        // 分析3: 计算购买转化率 (购买行为数 / 总行为数)
        long totalBehaviorCount = logDF.count();
        long buyBehaviorCount = logDF.filter("behavior_type = 'buy'").count();
        double conversionRate = (double) buyBehaviorCount / totalBehaviorCount;

        System.out.println("购买转化率: " + conversionRate);

        // 保存分析1：每个用户浏览次数（pv_count）
        pvCountDF.coalesce(1) // 合并成一个文件
                .write()
                .mode(SaveMode.Overwrite) // 如果有就覆盖
                .option("header", "true") // 带表头
                .csv("hdfs://20.33.32.192:9000/user/hadoop/pv_count_result");

        // 保存分析2：最热门商品Top10
        popularItemsDF.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv("hdfs://20.33.32.192:9000/user/hadoop/popular_items_result");

        // 保存分析3：购买转化率 —— 这个是单独的数，不是DataFrame
        // 需要手动写字符串
        List<String> conversionData = new ArrayList<>();
        conversionData.add("conversion_rate," + conversionRate);

        JavaRDD<String> conversionRDD = sc.parallelize(conversionData);
        conversionRDD.coalesce(1)
                .saveAsTextFile("hdfs://20.33.32.192:9000/user/hadoop/conversion_rate_result");


        // 关闭Spark Context
        sc.close();
        spark.stop();
    }

    /**
     * 随机生成用户行为日志数据
     * @param numLogs 日志数量
     * @param behaviorTypes 行为类型
     * @return 日志数据列表
     */
    private static List<String> generateLogData(int numLogs, String[] behaviorTypes) {
        List<String> logData = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < numLogs; i++) {
            int userId = random.nextInt(1000) + 1; // user_id: 1-1000
            int itemId = random.nextInt(5000) + 1; // item_id: 1-5000
            int categoryId = random.nextInt(1000) + 1; // category_id: 1-1000
            String behaviorType = behaviorTypes[random.nextInt(behaviorTypes.length)];
            String timestamp = "2023-01-01 " + String.format("%02d:%02d:%02d", random.nextInt(24), random.nextInt(60), random.nextInt(60));
            logData.add(userId + "," + itemId + "," + categoryId + "," + behaviorType + "," + timestamp);
        }
        return logData;
    }
}
