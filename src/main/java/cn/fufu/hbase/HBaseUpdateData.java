package cn.fufu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUpdateData {
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
        Put put = new Put(Bytes.toBytes("29034")); //rowkey
        // 更新单元格数据
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("M&M"));
        table.put(put);

        if (table!=null) {
            table.close();
        }
        if (admin!=null) {
            admin.close();
        }
        if (connection!=null) {
            connection.close();
        }

    }
}
