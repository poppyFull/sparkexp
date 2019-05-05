package cn.fufu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HBaseTest {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("test"));
        Scan scan = new Scan();
        // scan.setStartRow(Bytes.toBytes(startRow));
        // scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            showCell(result);
        }
        table.close();
    }

    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowKey:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("Col:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }
}
