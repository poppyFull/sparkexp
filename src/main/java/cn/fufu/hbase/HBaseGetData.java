package cn.fufu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseGetData {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        String tableName = "t4";
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("no table_" + tableName + ", cant get!");
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes("klpn4")); //rowkey
        get.addFamily(Bytes.toBytes("info")); // colFamily
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name")); //addColumn(colFamily, col)
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
        Result result = table.get(get);
        System.out.println(result);
        new HBaseTest().showCell(result);
        if (table!=null) {
            table.close();
        }
        if (admin!=null)
            admin.close();
        if (connection!=null)
            connection.close();

    }
}
