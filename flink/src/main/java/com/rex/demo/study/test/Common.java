package com.rex.demo.study.test;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * @Author li zhiqang
 * @create 2020/11/24
 */
public class Common {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> text = env.readTextFile("hdfs://hadoop-master:9000/test/data/json.txt");
        text.print();

        /**
         * 异常的原因就是说，自上次执行以来，没有定义新的数据接收器。对于离线批处理的算子，如：“count()”、“collect()”或“print()”等既有sink功能，还有触发的功能。我们上面调用了print()方法，会自动触发execute，所以最后面的一行执行器没有数据可以执行。
         * 对于学过sparkstreaming后初学flink的小伙伴来说，这里要注意一下，有点小区别。
         */
//        env.execute();
    }

}
