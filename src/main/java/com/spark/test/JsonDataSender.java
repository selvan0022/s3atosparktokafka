package com.spark.test;

import com.spark.test.kafka.KafkaProducerService;
import com.spark.test.pipeline.DataPipeline;
import com.spark.test.spark.S3Configuration;
import com.spark.test.spark.S3DataProcessorService;
import org.apache.spark.sql.SparkSession;

/**
 * Main application class - provides S3 to Kafka pipeline as a service
 * External parties should call: processPipeline(s3Path, kafkaBroker, kafkaTopic, awsAccessKey, awsSecretKey)
 */
public class JsonDataSender {
    private static final String SPARK_HOME = "/usr/local/spark/spark-2.4.4-bin-hadoop2.6";
    private static final String APP_NAME = "S3 to Kafka Pipeline";
    private static final long DEFAULT_PROCESSING_DELAY_MS = 500;

    /**
     * Main entry point for processing S3 data through Spark and sending to Kafka
     * This is the primary method for external parties to call
     * 
     * @param s3Path S3 path (e.g., s3a://bucket-name/path/to/file.csv)
     * @param kafkaBroker Kafka broker address (e.g., localhost:9092)
     * @param kafkaTopic Kafka topic name
     * @param awsAccessKey AWS access key for S3
     * @param awsSecretKey AWS secret key for S3
     * @throws Exception if pipeline execution fails
     */
    public static void processPipeline(String s3Path, String kafkaBroker, String kafkaTopic, 
                                       String awsAccessKey, String awsSecretKey) throws Exception {
        processPipeline(s3Path, kafkaBroker, kafkaTopic, awsAccessKey, awsSecretKey, DEFAULT_PROCESSING_DELAY_MS);
    }

    /**
     * Main entry point with custom processing delay
     * 
     * @param s3Path S3 path (e.g., s3a://bucket-name/path/to/file.csv)
     * @param kafkaBroker Kafka broker address (e.g., localhost:9092)
     * @param kafkaTopic Kafka topic name
     * @param awsAccessKey AWS access key for S3
     * @param awsSecretKey AWS secret key for S3
     * @param processingDelayMs Delay between processing records in milliseconds
     * @throws Exception if pipeline execution fails
     */
    public static void processPipeline(String s3Path, String kafkaBroker, String kafkaTopic,
                                       String awsAccessKey, String awsSecretKey, long processingDelayMs) throws Exception {
        // Set Spark home if needed
        System.setProperty("spark.home", SPARK_HOME);

        System.out.println("\n" + repeatString("=", 70));
        System.out.println("📊 S3 to Kafka Pipeline");
        System.out.println(repeatString("=", 70));
        System.out.println("Configuration:");
        System.out.println("  Kafka Broker: " + kafkaBroker);
        System.out.println("  Kafka Topic: " + kafkaTopic);
        System.out.println("  S3 Path: " + s3Path);
        System.out.println("  Processing Delay: " + processingDelayMs + "ms");
        System.out.println(repeatString("=", 70));

        DataPipeline pipeline = null;

        try {
            // Create Spark Session
            SparkSession spark = SparkSession.builder()
                    .appName(APP_NAME)
                    .master("local[*]")
                    .config("spark.driver.host", "localhost")
                    .getOrCreate();

            // Initialize services
            KafkaProducerService kafkaProducer = new KafkaProducerService(kafkaBroker, kafkaTopic);
            kafkaProducer.initialize();

            S3Configuration s3Config = new S3Configuration(
                    awsAccessKey,
                    awsSecretKey,
                    s3Path,
                    "s3.us-east-1.amazonaws.com"
            );

            S3DataProcessorService s3Processor = new S3DataProcessorService(spark, s3Config);

            // Create and execute pipeline
            pipeline = new DataPipeline(s3Processor, kafkaProducer, processingDelayMs);
            pipeline.execute();

        } finally {
            // Cleanup
            if (pipeline != null) {
                pipeline.cleanup();
            }
            System.out.println("\n" + repeatString("=", 70));
            System.out.println("Pipeline completed!");
            System.out.println(repeatString("=", 70));
        }
    }

    public static void main(String[] args) {
        try {
            // Example usage for testing
            processPipeline(
                    "s3a://your-bucket-name/path/to/your/file.csv",
                    "localhost:9092",
                    "spark-data-topic",
                    "YOUR_AWS_ACCESS_KEY",
                    "YOUR_AWS_SECRET_KEY"
            );
        } catch (Exception e) {
            System.err.println("\n✗ Application failed!");
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Helper method to repeat a string (Java 8 compatible alternative to String.repeat())
     */
    private static String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}
