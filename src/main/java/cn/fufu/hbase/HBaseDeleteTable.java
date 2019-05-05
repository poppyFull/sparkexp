package cn.fufu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseDeleteTable {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        // delete table
        String tableName = "t3";
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println(tableName + " be deleted successfully");
        }
        else {
            System.out.println("no table_" + tableName + " can be deleted!");
        }
        if (admin!=null)
            admin.close();
        if (connection!=null)
            connection.close();

    }
}
