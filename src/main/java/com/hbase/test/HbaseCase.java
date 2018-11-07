package com.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Set;

/**
 * Created by xuxp on 2017/7/14.
 */
public class HbaseCase {
    public static Connection connection;
    public static Admin admin;
    public static Configuration configuration;
    private static Set<byte[]> columnQualifiers;

    public static void main(String[] args) throws IOException {

        init();
//        createTable("Student", new String[]{"info","grade"});
//        addRecord("Student", "Zhangsan", new String[]{"info:S_No", "info:S_Sex", "info:S_Age"}, new String[]{"2015001", "male", "23"});
//        addRecord("Student", "Mary", new String[]{"info:S_No", "info:S_Sex", "info:S_Age"}, new String[]{"2015002", "female", "22"});
//        addRecord("Student", "Lisi", new String[]{"info:S_No", "info:S_Sex", "info:S_Age"}, new String[]{"2015003", "male", "24"});
//        addRecord("Student", "Zhangsan", new String[]{"grade:C_N0", "grade:C_Name", "grade:C_Credit", "grade:C_Score"}, new String[]{"2015001", "Math", "2.0", "86"});
//        addRecord("Student", "Zhangsan", new String[]{"grade:C_N0", "grade:C_Name", "grade:C_Credit", "grade:C_Score"}, new String[]{"2015001", "English", "3.0", "69"});
//        addRecord("Student", "Mary", new String[]{"grade:C_N0", "grade:C_Name", "grade:C_Credit", "grade:C_Score"}, new String[]{"2015002", "Computer Science", "5.0", "77"});
//        addRecord("Student", "Mary", new String[]{"grade:C_N0", "grade:C_Name", "grade:C_Credit", "grade:C_Score"}, new String[]{"2015002", "English", "3.0", "99"});
//        addRecord("Student", "Lisi", new String[]{"grade:C_N0", "grade:C_Name", "grade:C_Credit", "grade:C_Score"}, new String[]{"2015003", "Math", "2.0", "98"});
//        addRecord("Student", "Lisi", new String[]{"grade:C_N0", "grade:C_Name", "grade:C_Credit", "grade:C_Score"}, new String[]{"2015004", "Computer Science", "5.0", "95"});

//        createTable("Course",new String[]{"info"});
//        addRecord("Course", "123001", new String[]{"info:C_Name", "info:C_Credit"}, new String[]{"Math", "2.0"});
//        addRecord("Course", "123002", new String[]{"info:C_Name", "info:C_Credit"}, new String[]{"Computer Science", "5.0"});
//        addRecord("Course", "123003", new String[]{"info:C_Name", "info:C_Credit"}, new String[]{"English", "3.0"});
//        scanColumn("Course","info");
//        scanColumn("Course","info:C_Name");
//        modifyData("Course", "123001", "info:C_Credit", "3.0");
//        scanColumn("Course", "info:C_Credit");
        deleteRow("Course","Course");
        close();
    }

    public static void init() {
//        System.out.println("connect begin");
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "file:///usr/local/hbase/hbase-tmp");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
//            System.out.println("connect ok");
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
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete old table");
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        for (String str : colFamily) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        close();
    }

    public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(row));
        for (int i = 0; i < values.length; i++) {
            String colFamily = fields[i].split(":")[0];
            String col = fields[i].split(":")[1];
            String val = values[i];
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        }
        table.put(put);
        table.close();
    }

    public static void modifyData(String tablename, String row, String column, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tablename));
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(column.split(":")[0]), Bytes.toBytes(column.split(":")[1]), Bytes.toBytes(value));
        table.put(put);
    }

    public static void scanColumn(String tableName, String column) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
//        System.out.println("Table Name : " + tableName);
        Scan scan = new Scan();
        String columnFamily = null;

        if (column.split(":").length == 1) {
            scan.addFamily(Bytes.toBytes(column));
        } else {
//            scan.addFamily(Bytes.toBytes(column.split(":")[0]));
            scan.addColumn(Bytes.toBytes(column.split(":")[0]), Bytes.toBytes(column.split(":")[1]));
        }
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
//                System.out.println(new String(r.getRow()));
            for (KeyValue kv : r.list()
            ) {
                System.out.println("row" + Bytes.toString(kv.getRow()));
                System.out.println("family:"
                        + Bytes.toString(kv.getFamily()));
                System.out.println("qualifier:"
                        + Bytes.toString(kv.getQualifier()));
                System.out
                        .println("value:" + Bytes.toString(kv.getValue()));
                System.out.println("timestamp:" + kv.getTimestamp());
                System.out
                        .println("-------------------------------------------");
            }
        }

    }

    public static void deleteRow(String tableName, String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(row));
        table.delete(delete);
    }

    public static void getResultScann(String tableName) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = null;
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
//                for (KeyValue kv : r.list()) {
//                    System.out.println("row:" + Bytes.toString(kv.getRow()));
//                    System.out.println("family:" + Bytes.toString(kv.getFamily()));
//                    System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
//                    System.out.println("value:" + Bytes.toString(kv.getValue()));
//                    System.out.println("timestamp:" + kv.getTimestamp());
//                    System.out.println("-------------------------------------------");
                showCell(r);
            }
        } finally {
            rs.close();
        }
    }

    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }
}

