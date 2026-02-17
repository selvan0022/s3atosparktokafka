package com.spark.test.spark;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * Service class for reading and processing data from S3 using Spark
 */
public class S3DataProcessorService {
    private final SparkSession sparkSession;
    private final S3Configuration s3Config;

    /**
     * Constructor
     * @param sparkSession Spark session instance
     * @param s3Config S3 configuration
     */
    public S3DataProcessorService(SparkSession sparkSession, S3Configuration s3Config) {
        this.sparkSession = sparkSession;
        this.s3Config = s3Config;
    }

    /**
     * Read data from S3 bucket
     * @return Dataset of rows read from S3
     */
    public Dataset<Row> readFromS3() {
        System.out.println("\n📂 Reading data from S3...");
        System.out.println("  Bucket Path: " + s3Config.getS3Path());

        // Configure Hadoop for S3 access
        configureS3Access();

        // Read the data from S3 into a DataFrame
        Dataset<Row> df = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(s3Config.getS3Path());

        System.out.println("✓ Data loaded from S3 successfully!");
        System.out.println("  Total records: " + df.count());

        return df;
    }

    /**
     * Validate and filter corrupt records
     * @param df Input dataset
     * @return Filtered dataset with valid records only
     */
    public Dataset<Row> validateData(Dataset<Row> df) {
        System.out.println("\n🔍 Validating data...");
        df.printSchema();

        Dataset<Row> validRecords;
        String[] columns = df.columns();
        boolean hasCorruptColumn = false;

        // Check if corrupt record column exists
        for (String col : columns) {
            if ("_corrupt_record".equals(col)) {
                hasCorruptColumn = true;
                break;
            }
        }

        if (hasCorruptColumn) {
            long corruptCount = df.filter(df.col("_corrupt_record").isNotNull()).count();
            if (corruptCount > 0) {
                System.out.println("\n⚠️  WARNING: Found " + corruptCount + " corrupt records!");
                System.out.println("Corrupt records:");
                df.filter(df.col("_corrupt_record").isNotNull())
                        .select("_corrupt_record")
                        .show(false);
            }
            validRecords = df.filter(df.col("_corrupt_record").isNull());
        } else {
            validRecords = df;
        }

        System.out.println("✓ Valid records: " + validRecords.count());
        return validRecords;
    }

    /**
     * Process each row and return as list
     * @param validRecords Valid dataset
     * @return List of rows
     */
    public List<Row> processRecords(Dataset<Row> validRecords) {
        System.out.println("\n⚙️  Processing records...");
        List<Row> rows = validRecords.collectAsList();
        System.out.println("✓ Collected " + rows.size() + " records for processing");
        return rows;
    }

    /**
     * Convert Row to JSON string
     * @param row Row object
     * @return JSON string
     */
    public String rowToJson(Row row) {
        Gson gson = new Gson();
        return gson.toJson(row);
    }

    /**
     * Configure S3 access credentials in Hadoop configuration
     */
    private void configureS3Access() {
        Configuration hadoopConf = sparkSession.sparkContext().hadoopConfiguration();
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.access.key", s3Config.getAccessKey());
        hadoopConf.set("fs.s3a.secret.key", s3Config.getSecretKey());
        hadoopConf.set("fs.s3a.endpoint", s3Config.getEndpoint());

        System.out.println("✓ S3 configuration applied");
    }

    /**
     * Stop the Spark session
     */
    public void close() {
        if (sparkSession != null) {
            sparkSession.stop();
            System.out.println("✓ Spark session closed successfully!");
        }
    }
}
