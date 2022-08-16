package com.techienotes;

import com.techienotes.domain.Rate;
import com.techienotes.mapper.RateFlatMapGroupWithStateFunction;
import com.techienotes.mapper.RateGroupMapper;
import com.techienotes.mapper.RateMapGroupWithStateFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class SparkStreamingDriver {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        String sparkMaster = "local";
        SparkConf conf = new SparkConf()
                .setAppName("Spark-Streaming-Example")
                .setMaster(sparkMaster);

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

//        StructType customSchema = new StructType()
//                .add("timestamp", DataTypes.TimestampType, true)
//                .add("price", DataTypes.LongType, true)
//                .add("stock", DataTypes.StringType, true);

        Dataset<Row> dataset = spark.readStream()
                .format("rate")
//                .format("csv")
//                .option("header", true)
//                .option("path", "/directory_path")
//                .option("inferSchema", true)
//                .schema(customSchema)
                .load();

        dataset = dataset.withColumn("stock", functions.lit("msft"));
        Dataset<Rate> rateDataset = dataset.as(Encoders.bean(Rate.class));

        // Not supported in Streaming, It works for batch mode
//        dataset = dataset.withColumn("movingAverage", functions.avg(dataset.col("value")).over(Window.partitionBy("stock").rowsBetween(-49, 0)));

        KeyValueGroupedDataset<String, Rate> keyValueGroupedDataset = rateDataset.groupByKey(new RateGroupMapper(), Encoders.STRING());
//        Dataset<Rate> mapGroupsWithState = keyValueGroupedDataset.mapGroupsWithState(new RateMapGroupWithStateFunction()
//                , Encoders.bean(Rate.class), Encoders.bean(Rate.class)
//                , GroupStateTimeout.ProcessingTimeTimeout());

        Dataset<Rate> mapGroupsWithState = keyValueGroupedDataset.flatMapGroupsWithState(new RateFlatMapGroupWithStateFunction()
                , OutputMode.Update()
                , Encoders.bean(Rate.class)
                , Encoders.bean(Rate.class)
                , GroupStateTimeout.ProcessingTimeTimeout());

        StreamingQuery streamingQuery = mapGroupsWithState.writeStream()
                .format("console")
                .queryName("RateQuery")
                .option("checkpointLocation", "./checkpoint")
//                .outputMode(OutputMode.Append())
                .outputMode(OutputMode.Update())
                .trigger(Trigger.ProcessingTime("10 seconds"))
//                .trigger(Trigger.Continuous("1 second"))
                .start();

        streamingQuery.awaitTermination();
    }
}
