package com.techienotes;

import com.techienotes.domain.KafkaMessage;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class SparkKafkaDriver {
    private SparkSession sparkSession;

    public static void main(String[] args) {
        SparkKafkaDriver driver = new SparkKafkaDriver();
        driver.drive();
    }

    private void drive() {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Spark-SQL-Example")
                .set("spark.sql.sources.partitionColumnTypeInference.enabled", "false");
        sparkSession = SparkSession.builder().config(conf).getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");

        List<KafkaMessage> messageList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messageList.add(new KafkaMessage("key" + i, "value" + i));
        }

        Dataset<Row> dataFrame = sparkSession.createDataFrame(messageList, KafkaMessage.class);
        dataFrame.show();

        String brokerConfig = "localhost:9092";

        dataFrame.write()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokerConfig)
                .option("kafka.security.protocol", "PLAINTEXT")
                .option("topic", "stream-runner")
                .option("kafka.partitioner.class", "org.apache.kafka.clients.producer.RoundRobinPartitioner")
                .save();
    }
}
