package com.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
    public static void main(String[] args) {
        System.out.println("connect begin");


        System.out.println("connect ok");
        try {
            HTable table = new HTable(configuration, "tempTable");
            table.setAutoFlush(false);
            Put put = new Put(Bytes.toBytes("hello12"));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cf"), Bytes.toBytes("vale"));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void init(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","file:///usr/local/hbase/hbase-tmp");
        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void close(){
        try {
            if (admin!=null)
                admin.close();
            if (null!=connection)
                connection.close();
        }
        catch (IOException e){
            e.printStackTrace();
        }

    }
}