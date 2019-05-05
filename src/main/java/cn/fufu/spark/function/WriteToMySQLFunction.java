package cn.fufu.spark.function;

import org.apache.spark.api.java.function.VoidFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

public class WriteToMySQLFunction implements VoidFunction<Iterator<String>> {
    private String url = null;
    private String username = null;
    private String password = null;
    private Connection conn = null;
    private Statement stmt = null;

    public WriteToMySQLFunction(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public void call(Iterator<String> stringIterator) throws Exception {
        try {
            if (conn == null) {
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection(url, username, password);
                stmt = conn.createStatement();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        while (stringIterator.hasNext()) {
            String lines = stringIterator.next();
            String time = lines.substring(0, 23);
            String content = lines.substring(24);
            String sql = "insert into testdb.warninfo(time, content) values(\"" + time + "\", \"" + content + "\")";
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
