package com.techienotes;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DrawingBoardDriver {
    public static void main(String[] args) {
        String sparkMaster = "local";
        SparkConf conf = new SparkConf().setAppName("Spark-SQL-Example")
                .setMaster(sparkMaster);
        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("yAxis", DataTypes.createArrayType(DataTypes.StringType), true));
        fields.add(DataTypes.createStructField("xAxis", DataTypes.createArrayType(DataTypes.StringType), true));
        fields.add(DataTypes.createStructField("value", DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.DoubleType)), true));
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowJavaRDD = javaSparkContext.parallelize(
                Arrays.asList(
                        RowFactory.create(1, "point1", new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, new String[]{"A", "B", "C", "D", "E"}, new Double[][]{
                                new Double[]{0.0, 0.0, 0.20, 0.0, 0.10}
                                , new Double[]{0.0, 0.0, 0.0, 0.0, 0.0}
                                , new Double[]{0.0, 0.10, 0.0, 0.10, 0.0}
                                , new Double[]{0.0, 0.0, 0.0, 0.0, 0.0}
                                , new Double[]{0.0, 0.0, 0.30, 0.0, 0.40}
                                , new Double[]{0.0, 0.0, 0.0, 0.0, 0.0}
                                , new Double[]{0.20, 0.0, 0.20, 0.0, 0.0}
                                , new Double[]{0.0, 0.0, 0.0, 0.0, 0.0}
                                , new Double[]{0.0, 0.0, 0.0, 0.20, 0.0}
                                , new Double[]{0.0, 0.05, 0.0, 0.06, 0.0}})
                ));
        Dataset<Row> dataFrame = sparkSession.createDataFrame(rowJavaRDD, schema);
        dataFrame.show(5, false);

        Seq<String> buffer = JavaConverters.asScalaBufferConverter(Arrays.asList("idx", "value2")).asScala();
        dataFrame = dataFrame.select(functions.col("*"), functions.posexplode(functions.col("value")).as(buffer));
        dataFrame.show();

        dataFrame.sqlContext().udf().register("combine_yAxis", new UDF2<WrappedArray<String>, Integer, String>() {
            @Override
            public String call(WrappedArray<String> stringWrappedArray, Integer integer) throws Exception {
                return stringWrappedArray.apply(integer);
            }
        }, DataTypes.StringType);
        dataFrame = dataFrame.withColumn("yAxis", functions.callUDF("combine_yAxis", functions.col("yAxis"), functions.col("idx")));
        dataFrame.show();

//        dataFrame = dataFrame.withColumn("zipped", functions.arrays_zip(functions.col("yAxis"), functions.col("xAxis")));
        dataFrame.sqlContext().udf().register("array_zip", new UDF2<WrappedArray<String>, WrappedArray<Double>, List<String[]>>() {
            @Override
            public List<String[]> call(WrappedArray<String> stringWrappedArray, WrappedArray<Double> doubleWrappedArray) throws Exception {
                return IntStream.range(0, stringWrappedArray.size()).mapToObj(i ->
                        new String[]{
                                stringWrappedArray.apply(i) == null ? null : stringWrappedArray.apply(i),
                                doubleWrappedArray.apply(i) == null ? null : doubleWrappedArray.apply(i).toString()
                        }).collect(Collectors.toList());
            }
        }, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType)));
        dataFrame = dataFrame.withColumn("zipped", functions.callUDF("array_zip", functions.col("xAxis"), functions.col("value2")));
        dataFrame.show();

        dataFrame = dataFrame.withColumn("explode", functions.explode(functions.col("zipped")));

        dataFrame.sqlContext().udf().register("createColumns", new UDF1<WrappedArray<String>, Row>() {
            @Override
            public Row call(WrappedArray<String> stringWrappedArray) throws Exception {
                return RowFactory.create(stringWrappedArray.apply(0), Double.parseDouble(stringWrappedArray.apply(1)));
            }
        }, DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("x", DataTypes.StringType, true), DataTypes.createStructField("val", DataTypes.DoubleType, true))));
        dataFrame = dataFrame.withColumn("combined", functions.callUDF("createColumns", functions.col("explode")));
        dataFrame = dataFrame.withColumn("xAxis", functions.col("combined.x"));
        dataFrame = dataFrame.withColumn("value", functions.col("combined.val"));

        dataFrame = dataFrame.drop("idx", "value2", "zipped", "explode", "combined");
        dataFrame.show();
        dataFrame.filter(functions.col("value").$greater(0.0)).show();
        System.out.println(dataFrame.count());
    }
}
