package cn.fufu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBasePutData {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        String tableName = "t4";
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("no table_" +tableName + ", cant put data!");
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 插入单条数据
        Put put = new Put(Bytes.toBytes("29034")); // rowkey
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("meow")); // addColumn(colFamily, col, value)
        table.put(put);

        // 批量插入数据
        List<Put> putList = new ArrayList<Put>();
        Put p = null;
        p = new Put(Bytes.toBytes("18932"));
        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Lily"));
        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes("female"));
        putList.add(p);
        p = new Put(Bytes.toBytes("69534"));
        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("JK"));
        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("16"));
        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes("female"));
        putList.add(p);
        p = new Put(Bytes.toBytes("klpn4"));
        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("QQ"));
        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("30"));
        p.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes("male"));
        putList.add(p);
        table.put(putList);
        if (table!=null) {
            table.close();
        }
        if (admin!=null)
            admin.close();
        if (connection!=null)
            connection.close();

    }
}
