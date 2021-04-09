package com.techienotes;

import com.techienotes.domain.FileInputLine;
import com.techienotes.exceptions.ValidationException;
import com.techienotes.utils.FileUtil;
import com.techienotes.utils.UDFUtil;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.techienotes.config.CustomConstants.COLUMN_DOUBLE_UDF_NAME;
import static com.techienotes.config.CustomConstants.COLUMN_UPPERCASE_UDF_NAME;
import static com.techienotes.config.CustomConstants.DOUBLED_COLUMN_NAME;
import static com.techienotes.config.CustomConstants.NAME_COLUMN_NAME;
import static com.techienotes.config.CustomConstants.NUMBER_COLUMN_NAME;
import static com.techienotes.config.CustomConstants.UPPSERCASE_NAME_COLUMN_NAME;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

class SparkJob {
    private final Logger logger = Logger.getLogger(SparkJob.class);

    private String[] args;
    private String inputFilePath;
    private UDFUtil udfUtil;
    private FileUtil fileUtil;

    SparkJob(String[] args) {
        this.args = args;
    }

    void startJob() throws ValidationException, IOException {

        /*
        Validate to check if we have all the required arguments.
         */
        validateArguments();

        /*
        Load all the properties from the .properties file and initialize the instance variables.
         */
        loadProperties();

        /*
        Register the required UDFs.
         */
        registerUdfs();

        /*
        Get the dataset from the input file.
         */
        Dataset<FileInputLine> inputFileDataset = fileUtil.getDatasetFromFile(inputFilePath);

        inputFileDataset.show();

        Dataset<Row> doubledColumnDataset = inputFileDataset.withColumn(DOUBLED_COLUMN_NAME,
                callUDF(COLUMN_DOUBLE_UDF_NAME, col(NUMBER_COLUMN_NAME)));

        doubledColumnDataset.show();

        Dataset<Row> upperCaseColumnDataset = doubledColumnDataset.withColumn(UPPSERCASE_NAME_COLUMN_NAME,
                callUDF(COLUMN_UPPERCASE_UDF_NAME, col(NAME_COLUMN_NAME)));

        upperCaseColumnDataset.show();
    }

    private void loadProperties() throws IOException {
        Properties properties = new Properties();
        String propFileName = "application.properties";

        InputStream inputStream = App.class.getClassLoader().getResourceAsStream(propFileName);

        try {
            properties.load(inputStream);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw e;
        }

        initialize(properties);
    }

    private void registerUdfs() {
        this.udfUtil.registerColumnDoubleUdf();
        this.udfUtil.registerColumnUppercaseUdf();
    }

    private void initialize(Properties properties) {

        String sparkMaster = properties.getProperty("spark.master");
        SparkConf conf = new SparkConf().setAppName("Spark-SQL-Example");
        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
//        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        SQLContext sqlContext = sparkSession.sqlContext();

        inputFilePath = this.args[0];

        udfUtil = new UDFUtil(sqlContext);
        fileUtil = new FileUtil(sparkSession);
    }

    private void validateArguments() throws ValidationException {

        if (args.length < 1) {
            logger.error("Invalid arguments.");
            logger.error("1. Input file path.");
            logger.error("Example: java -jar <jarFileName.jar> /path/to/input/file");

            throw new ValidationException("Invalid arguments, check help text for instructions.");
        }
    }
}
