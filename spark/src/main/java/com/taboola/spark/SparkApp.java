package com.taboola.spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;

import static org.apache.spark.sql.functions.col;

public class SparkApp {

    public static final int SIZE_LIST=10;

    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder().master("local[4]").getOrCreate();


        // generate events
        // each event has an id (eventId) and a timestamp
        // an eventId is a number between 0 an 99
        Dataset<Row> events = getEvents(spark);
        events.withWatermark("timestamp","1 minute");
//        events.write().

//        JavaRDD<Row> eventsRdd =  events.toJavaRDD();

//        eventsRdd.foreachPartition( it -> {
//                Row rowIterator = (Row) it;
//                String evt = (String) rowIterator.get(0);
//                String timeStamp = (String) rowIterator.get(1);
//                System.out.println("Event is "+evt+" : "+timeStamp);
//        });
        events.writeStream()
                .foreachBatch(new PostgresSink()).trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS)).start();
        // REPLACE THIS CODE
        // The spark stream continuously receives messages. Each message has 2 fields:
        // * timestamp
        // * event id (valid values: from 0 to 99)
        //
        // The spark stream should collect, in the database, for each time bucket and event id, a counter of all the messages received.
        spark.streams().awaitAnyTermination();
    }

    private static Dataset<Row> getEvents(SparkSession spark) {
        return spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", "10000")
                .load()
                .withColumn("eventId", functions.rand(System.currentTimeMillis()).multiply(functions.lit(100)).cast(DataTypes.LongType))
                .select("eventId", "timestamp");
    }

    private static class PostgresSink implements VoidFunction2<Dataset<Row>,Long> {
        @Override
        public void call(Dataset<Row> dataset, Long v2) throws Exception {
            dataset.show(10);
            writeToDBUsingRDD(dataset);
        }
    }

    private static void writeToDBUsingRDD(Dataset<Row> dataFrame) {

        //used spark query to accomplish the aggregation but if i had more time would like to do it with JavaRDD (map) grouping by timestamp intervals, and then reduce each grouping
        // to caculate the count that way when there is large amounts of data we can utilize the cluster and scale horizontally
        Dataset<Row> aggregation =dataFrame.groupBy(col("eventId") , functions.window(col("timestamp"), "5 second")).count().select("eventId", "window.start", "count")
                .withColumnRenamed("start","time_bucket")
                .withColumnRenamed("eventId", "event_id").orderBy("time_bucket");
        JavaRDD<Row> rdd = aggregation.toJavaRDD();
        aggregation.show();
        System.out.println("num partitions" +rdd.getNumPartitions());
        rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
            @Override
            public void call(Iterator<Row> it){
                Connection connection=null;
                try {
                    connection = DriverManager.getConnection("jdbc:hsqldb:hsql://localhost/xdb", "sa", "");
                    while (it.hasNext()) {
                        Row row = it.next();
                        PreparedStatement stmt = null;
                        //I just did this because its quick i should configure a datasource and connection pool and create a dao class but since this is just poc. In production I would
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
//                        System.out.println(row.getTimestamp(1).toString());
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
                System.out.println("Done");
            }
        });
    }

    private static void writeToDB(Dataset<Row> dataFrame) {
        // Write to JDBC DB
        Dataset<Row> aggregation =dataFrame.groupBy(col("eventId") , functions.window(col("timestamp"), "5 second")).count().select("eventId", "window.start", "count")
                .withColumnRenamed("start","time_bucket")
                .withColumnRenamed("eventId", "event_id");
//        aggregation = aggregation.join(dataFrame, "eventId");

        aggregation.show();
        persistToDB(aggregation);
    }

    private static void persistToDB(Dataset<Row> dataFrame){
        String url = "jdbc:hsqldb:hsql://localhost/xdb";
        String table = "EVENTS";
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("Driver", "org.hsqldb.jdbc.JDBCDriver");
        connectionProperties.setProperty("user", "sa");
        connectionProperties.setProperty("password", "");
        dataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);
    }

}
