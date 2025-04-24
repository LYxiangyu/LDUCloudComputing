package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class App {
    static Configuration conf = HBaseConfiguration.create();
    static Connection connection;
    static String table_name = "student_sports";
    static String columnFamily1 = "basic_info";
    static String columnFamily2 = "score";
    static String[] columnFamilys = {"basic_info", "score"};

    public static void main(String[] args) {
        try {
            App.getConnect();
            App.createTable(table_name);
            App.addData(table_name);
            App.getAllRecord(table_name);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void getConnect() throws IOException {
        conf.set("hbase.rootdir", "hdfs://master:9000/hbase");
        conf.set("hbase.zookeeper.property.dataDir", "/usr/local/zookeeper/data");
        conf.set("hbase.cluster.distributed", "true");
        conf.set("hbase.zookeeper.quorum", "master,hadoop02,hadoop03");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void createTable(String tablename) throws Exception {
        TableName tableName = TableName.valueOf(tablename);
        Admin admin = connection.getAdmin();

        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println(tablename + " table exists, deleted...");
        }

        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
        for (String columnFamily : columnFamilys) {
            ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(columnFamily))
                    .build();
            tdb.setColumnFamily(cfd);
        }

        TableDescriptor td = tdb.build();
        admin.createTable(td);
        admin.close();

        System.out.println("create table success!");
    }

    public static void addData(String tablename) throws Exception {
        HTable table = (HTable) connection.getTable(TableName.valueOf(tablename));

        Put p1 = new Put(Bytes.toBytes("410080608"));
        p1.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("name"), Bytes.toBytes("Tom"));
        p1.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("height"), Bytes.toBytes("180cm"));
        p1.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("weight"), Bytes.toBytes("75kg"));
        p1.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes("standingbj"), Bytes.toBytes("2.12m"));
        p1.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes("running"), Bytes.toBytes("04:32"));

        Put p2 = new Put(Bytes.toBytes("410080728"));
        p2.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("name"), Bytes.toBytes("Alex"));
        p2.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("height"), Bytes.toBytes("165cm"));
        p2.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("weight"), Bytes.toBytes("53.1kg"));
        p2.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes("standingbj"), Bytes.toBytes("1.56m"));
        p2.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes("running"), Bytes.toBytes("06:12"));

        List<Put> puts = new ArrayList<>();
        puts.add(p1);
        puts.add(p2);
        table.put(puts);
        table.close();
    }

    public static void getAllRecord(String tableName) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);

            for (Result r : ss) {
                String rowKey = Bytes.toString(r.getRow());
                System.out.println("row key:" + rowKey);
                Cell[] cells = r.rawCells();

                for (Cell cell : cells) {
                    System.out.println(
                            Bytes.toString(cell.getQualifierArray(),
                                    cell.getQualifierOffset(),
                                    cell.getQualifierLength()) + "::" +
                                    Bytes.toString(cell.getValueArray(),
                                            cell.getValueOffset(),
                                            cell.getValueLength())
                    );
                }
                System.out.println("**********************************");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
