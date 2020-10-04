package com.taboola.spark;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PostgresSink implements VoidFunction2<Dataset<Row>,Long> {
        @Override
        public void call(Dataset<Row> dataset, Long v2) throws Exception {
            dataset.show(10);
            WriteService writeService = new WriteService();
            writeService.writeToDBRdd(dataset);
        }
}