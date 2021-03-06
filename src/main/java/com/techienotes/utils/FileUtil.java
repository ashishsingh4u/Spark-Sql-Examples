package com.techienotes.utils;

import com.techienotes.domain.FileInputLine;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class FileUtil {

    private final SparkSession sparkSession;

    public FileUtil(SparkSession _sparkSession) {
        this.sparkSession = _sparkSession;
    }

    public Dataset<FileInputLine> getDatasetFromFile(String filePath) {

        Dataset<FileInputLine> fileDataSet = this.sparkSession.read()
                .option("inferSchema", "true")
                .option("header", "true").csv(filePath)
                .as(Encoders.bean(FileInputLine.class));

        return fileDataSet;
    }
}
