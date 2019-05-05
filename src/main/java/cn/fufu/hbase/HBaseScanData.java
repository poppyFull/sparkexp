package cn.fufu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

// 批量查找数据
public class HBaseScanData {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        String tableName = "t4";
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("no table_" + tableName + ", cant scan!");
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("10")); // rowkey
        scan.setStopRow(Bytes.toBytes("60"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name")); // addcolumn（family, col）
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result: resultScanner) {
            System.out.println(result);
            new HBaseTest().showCell(result);
        }
        if (table!=null) {
            table.close();
        }
        if (admin!=null)
            admin.close();
        if (connection!=null)
            connection.close();

    }
}
