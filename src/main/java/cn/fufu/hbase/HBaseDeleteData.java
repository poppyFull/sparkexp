package cn.fufu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseDeleteData {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        String tableName = "t4";
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("no table_" +tableName + ", cant delete data!");
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = null; // rowkey
        // 删除指定列族
//        delete = new Delete(Bytes.toBytes("69534"));
//        delete.addFamily(Bytes.toBytes("info"));
//        delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
//        table.delete(delete);

        //批量删除
        List<Delete> deleteList = new ArrayList<Delete>();
        delete = new Delete(Bytes.toBytes("69534"));
        delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
        delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"));
        deleteList.add(delete);
        table.delete(deleteList);
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
