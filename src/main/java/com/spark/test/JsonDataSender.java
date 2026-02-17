package com.poc.spark;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.poc.domain.Employee;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Java application that reads JSON file and sends data one by one using Spark
 */
public class JsonDataSender {

    public static void main(String[] args) {
        // Set Spark home if needed
        String sparkHome = "/usr/local/spark/spark-2.4.4-bin-hadoop2.6";
        System.setProperty("spark.home", sparkHome);

        // Parse command line arguments
        String jsonFilePath = "data/sample-data.json";
        if (args.length > 0) {
            jsonFilePath = args[0];
        }

        System.out.println("=================================================");
        System.out.println("Spark JSON Data Sender Application");
        System.out.println("=================================================");
        System.out.println("Reading JSON file: " + jsonFilePath);

        // Create Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("JSON Data Sender")
                .master("local[*]") // Run locally with all available cores
                .config("spark.driver.host", "localhost")
                .getOrCreate();

        try {
            // Method 1: Using Spark SQL to read JSON
            System.out.println("\n--- Method 1: Using Spark SQL ---");
            readAndProcessWithSparkSQL(spark, jsonFilePath);

            // Method 2: Using manual JSON parsing and RDD
            System.out.println("\n--- Method 2: Using Manual Parsing ---");
            readAndProcessManually(spark, jsonFilePath);

            // Method 3: Read from S3 bucket
            System.out.println("\n--- Method 3: Read from S3 bucket ---");
            readAndPushtoKafka(spark, jsonFilePath);

        } catch (Exception e) {
            System.err.println("Error processing JSON file: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Stop Spark session
            spark.stop();
            System.out.println("\n=================================================");
            System.out.println("Application completed successfully!");
            System.out.println("=================================================");
        }
    }

    /**
     * Read and process S3 using Spark SQL
     */
    private static void readAndPushtoKafka(SparkSession spark) {
        String awsAccessKey = "YOUR_AWS_ACCESS_KEY";
        String awsSecretKey = "YOUR_AWS_SECRET_KEY";
        String s3BucketPath = "s3a://your-bucket-name/path/to/your/file.csv"; // Use s3a:// protocol
        String s3Endpoint = "s3.us-east-1.amazonaws.com"; // Set the S3 endpoint (optional, depending on your setup)


        // Configure Hadoop for S3 access
        Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"); // Specify S3AFileSystem
        hadoopConf.set("fs.s3a.access.key", awsAccessKey);
        hadoopConf.set("fs.s3a.secret.key", awsSecretKey);
        // Optional: set the S3 endpoint if not using standard AWS regions, e.g., for
        // MinIO or a specific region
        hadoopConf.set("fs.s3a.endpoint", s3Endpoint);

        // Read the data from S3 into a DataFrame
        // This example reads a CSV file with a header and infers the schema.
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true") // Assumes the file has a header
                .option("inferSchema", "true") // Infers data types
                .load(s3BucketPath);

        // Show the data
        readAndSendData(df);
    }

    /**
     * Read and process JSON using Spark SQL
     */
    private static void readAndProcessWithSparkSQL(SparkSession spark, String jsonFilePath) {
        // Read JSON file using Spark SQL with multiline support and permissive mode
        Dataset<Row> df = spark.read()
                .option("multiline", "true")
                .option("mode", "PERMISSIVE") // Handles corrupt records gracefully
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .json(jsonFilePath);

        System.out.println("Total records: " + df.count());
        System.out.println("\nDataset Schema:");
        readAndSendData(df);
    }

    private static void readAndSendData(Dataset<Row> df) {
        df.printSchema();

        // Check if _corrupt_record column exists (only created when there are corrupt
        // records)
        Dataset<Row> validRecords;
        String[] columns = df.columns();
        boolean hasCorruptColumn = false;

        for (String col : columns) {
            if ("_corrupt_record".equals(col)) {
                hasCorruptColumn = true;
                break;
            }
        }

        if (hasCorruptColumn) {
            // Check for corrupt records
            long corruptCount = df.filter(df.col("_corrupt_record").isNotNull()).count();
            if (corruptCount > 0) {
                System.out.println("\n⚠️  WARNING: Found " + corruptCount + " corrupt records!");
                System.out.println("Corrupt records:");
                df.filter(df.col("_corrupt_record").isNotNull())
                        .select("_corrupt_record")
                        .show(false);
            }

            // Filter out corrupt records and process only valid ones
            validRecords = df.filter(df.col("_corrupt_record").isNull());
        } else {
            // No corrupt records, use all data
            validRecords = df;
        }

        System.out.println("\nValid records to process: " + validRecords.count());

        // Process each record one by one
        System.out.println("\nProcessing records one by one:");
        System.out.println("--------------------------------------------------");

        List<Row> rows = validRecords.collectAsList();
        int counter = 1;

        for (Row row : rows) {
            System.out.println("\n[Record " + counter + "]");
            System.out.println("  ID: " + row.getAs("id"));
            System.out.println("  Name: " + row.getAs("name"));
            System.out.println("  Email: " + row.getAs("email"));
            System.out.println("  Age: " + row.getAs("age"));
            System.out.println("  Department: " + row.getAs("department"));

            // Simulate sending data one by one
            sendData(row);

            counter++;

            // Add delay to simulate real-time processing
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Read and process JSON manually using Gson
     */
    private static void readAndProcessManually(SparkSession spark, String jsonFilePath) {
        try {
            // Parse JSON file using Gson
            JsonParser parser = new JsonParser();
            JsonArray jsonArray = parser.parse(new FileReader(jsonFilePath)).getAsJsonArray();

            List<String> jsonStrings = new ArrayList<>();
            for (JsonElement element : jsonArray) {
                jsonStrings.add(element.toString());
            }

            // Create RDD from JSON strings
            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
            JavaRDD<String> jsonRDD = jsc.parallelize(jsonStrings);

            System.out.println("Total records in RDD: " + jsonRDD.count());

            // Process each JSON string
            System.out.println("\nProcessing JSON records:");
            System.out.println("--------------------------------------------------");

            List<String> records = jsonRDD.collect();
            int counter = 1;

            Gson gson = new Gson();
            for (String record : records) {
                // Parse each record
                Employee employee = gson.fromJson(record, Employee.class);

                System.out.println("\n[JSON Record " + counter + "]");
                System.out.println("  Raw JSON: " + record);
                System.out.println("  Parsed -> " + employee);

                // Send data
                sendDataAsJson(record);

                counter++;

                // Add delay
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

        } catch (Exception e) {
            System.err.println("Error in manual processing: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Simulate sending data (Row object)
     */
    private static void sendData(Row row) {
        // Here you would implement your actual data sending logic
        // For example: send to Kafka, HTTP endpoint, database, etc.
        System.out.println("  >> Data sent successfully!");
    }

    /**
     * Simulate sending data (JSON string)
     */
    private static void sendDataAsJson(String jsonData) {
        // Here you would implement your actual data sending logic
        // For example: send to Kafka, HTTP endpoint, database, etc.
        System.out.println("  >> JSON data sent successfully!");
    }
}
