package cn.fufu.spark.function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Iterator;

public class LogToHBase implements VoidFunction<Iterator<String>> {
    private Connection connection = null;
    private String tableName = null;
    private Table table = null;

    public LogToHBase(String tableName) {
        this.tableName = tableName;
    }

    public void call(Iterator<String> stringIterator) throws Exception {
        if (connection == null) {
            Configuration confiuration = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(confiuration);
            this.table = connection.getTable(TableName.valueOf(tableName));
        }
        while (stringIterator.hasNext()) {
            String lines = stringIterator.next();
            String rowkey = lines.substring(0, 23);
            String value = lines.substring(24);
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes("type"), Bytes.toBytes("log"), Bytes.toBytes(value));
            table.put(put);
        }
        if (table!=null) {
            table.close();
        }
        if (connection!=null) {
            connection.close();
        }
    }
}
