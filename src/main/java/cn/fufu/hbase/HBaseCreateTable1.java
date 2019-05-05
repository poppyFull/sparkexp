package cn.fufu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseCreateTable1 {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        // create HBase table without splitKeys
        String tableName = "t3";
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + " already exist!");
        }
        else {
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            desc.addFamily(new HColumnDescriptor("private"));
            desc.addFamily(new HColumnDescriptor("public"));
            admin.createTable(desc);
            System.out.println(tableName + " be created successfully");
        }
        if (admin!=null)
            admin.close();
        if (connection!=null)
            connection.close();

    }
}
