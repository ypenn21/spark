package com.taboola.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;

import static org.apache.spark.sql.functions.col;

public class WriteService implements Serializable {

    public void writeToDBRdd(Dataset<Row> dataFrame) {

        // used spark query to accomplish the aggregation but if i had more time would like to do it with JavaRDD (map) grouping by timestamp intervals, and then reduce each grouping
        // to caculate the count that way when there is large amounts of data we can utilize the cluster and scale horizontally
        Dataset<Row> aggregation =dataFrame.groupBy(col("eventId") , functions.window(col("timestamp"), "5 second")).count().select("eventId", "window.start", "count")
                .withColumnRenamed("start","time_bucket")
                .withColumnRenamed("eventId", "event_id").orderBy("time_bucket");
        aggregation.repartition(col("time_bucket"));
        JavaRDD<Row> rdd = aggregation.toJavaRDD();
        aggregation.show();
        System.out.println("num partitions" +rdd.getNumPartitions());
        rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
            @Override
            public void call(Iterator<Row> it){
                Connection connection=null;
                try {
                    connection = DataSource.getConnection();
                    while (it.hasNext()) {
                        Row row = it.next();
                        PreparedStatement stmt = null;
                        //ToDO Water mark set for 1 minutefor so for these delayed message handling we can also query on event_id and time_bucket to
                        // see if it exists and if it does update the count by adding it to existing count
                        String sql = "INSERT INTO EVENTS (event_id, time_bucket, count) " +
                                "VALUES (?, ?, ?)";
                        try {
                            stmt = connection.prepareStatement(sql);
                            stmt.setLong(1, row.getLong(0));
                            stmt.setTimestamp(2, row.getTimestamp(1));
                            stmt.setLong(3, row.getLong(2));
                            stmt.executeUpdate();
                        } finally {
                            if (stmt != null) {
                                stmt.close();
                            }
                            //System.out.println(row.getTimestamp(1)+" "+row.get(0).toString());
                        }
                    }

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } finally{
                    if(connection!=null) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                //System.out.println("Done");
            }
        });
    }

    //not used
    public void writeToDB(Dataset<Row> dataFrame){
        Dataset<Row> aggregation =dataFrame.groupBy(col("eventId") , functions.window(col("timestamp"), "5 second")).count().select("eventId", "window.start", "count")
                .withColumnRenamed("start","time_bucket")
                .withColumnRenamed("eventId", "event_id");

        aggregation.show();
        String url = "jdbc:hsqldb:hsql://localhost/xdb";
        String table = "EVENTS";
        // Write to DB JDBC
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("Driver", "org.hsqldb.jdbc.JDBCDriver");
        connectionProperties.setProperty("user", "sa");
        connectionProperties.setProperty("password", "");
        aggregation.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);
    }
}
