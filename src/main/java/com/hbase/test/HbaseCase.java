package com.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by xuxp on 2017/7/14.
 */
public class HbaseCase {
    public static Connection connection;
    public static Admin admin;
    public static Configuration configuration;

    public static void main(String[] args) throws IOException{

        init();
        createTable("tempTable",null);
        close();
    }

    public static void init() {
        System.out.println("connect begin");
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "file:///usr/local/hbase/hbase-tmp");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            System.out.println("connect ok");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        try {
            if (admin != null)
                admin.close();
            if (null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void createTable(String myTableName, String[] colFamily) throws IOException {
        init();
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            System.out.println("table exist");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String str : colFamily) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            ;
        }
        close();
    }
}