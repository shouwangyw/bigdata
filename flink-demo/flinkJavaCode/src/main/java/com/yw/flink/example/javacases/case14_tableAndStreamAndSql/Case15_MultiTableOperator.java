package com.yw.flink.example.javacases.case14_tableAndStreamAndSql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

/**
 * Flink Table API 操作
 */
public class Case15_MultiTableOperator {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());

        Table t1 = tableEnv.fromValues(
                row(1, "zs", 18),
                row(1, "zs", 18),
                row(2, "ls", 19),
                row(2, "ls", 19),
                row(3, "ww", 20),
                row(4, "ml", 21),
                row(5, "tq", 22)
        );

        Table t2 = tableEnv.fromValues(
                row(1, "zs", 18),
                row(2, "ls", 19),
                row(2, "ls", 19),
                row(3, "ww", 20),
                row(4, "ml", 21),
                row(6, "gb", 23)
        );

        //union
//        Table union = t1.union(t2);
//        union.execute().print();
//        t1.unionAll(t2).execute().print();

//        t1.intersect(t2).execute().print();
//        t1.intersectAll(t2).execute().print();
//        t1.minus(t2).execute().print();
//        t1.minusAll(t2).execute().print();

        Table result = t1.select($("f0"), $("f1"), $("f2"))
                .where($("f0").in(2, 3,4));
        result.execute().print();
    }
}
