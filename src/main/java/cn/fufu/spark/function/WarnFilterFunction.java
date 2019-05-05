package cn.fufu.spark.function;

import org.apache.spark.api.java.function.Function;

public class WarnFilterFunction implements Function<String, Boolean> {
    public Boolean call(String s) throws Exception {
        return s.contains("WARN");
    }
}
