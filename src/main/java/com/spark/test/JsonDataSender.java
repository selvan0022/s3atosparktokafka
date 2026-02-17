package com.spark.test;

import com.spark.test.kafka.KafkaProducerService;
import com.spark.test.pipeline.DataPipeline;
import com.spark.test.spark.S3Configuration;
import com.spark.test.spark.S3DataProcessorService;
import org.apache.spark.sql.SparkSession;

/**
 * Main application class - orchestrates the S3 to Kafka pipeline
 */
public class JsonDataSender {
    // Spark configuration
    private static final String SPARK_HOME = "/usr/local/spark/spark-2.4.4-bin-hadoop2.6";
    private static final String APP_NAME = "S3 to Kafka Pipeline";

    // Kafka configuration
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "spark-data-topic";

    // S3 configuration
    private static final String AWS_ACCESS_KEY = "YOUR_AWS_ACCESS_KEY";
    private static final String AWS_SECRET_KEY = "YOUR_AWS_SECRET_KEY";
    private static final String S3_PATH = "s3a://your-bucket-name/path/to/your/file.csv";
    private static final String S3_ENDPOINT = "s3.us-east-1.amazonaws.com";

    // Processing configuration
    private static final long PROCESSING_DELAY_MS = 500;

    public static void main(String[] args) {
        // Set Spark home if needed
        System.setProperty("spark.home", SPARK_HOME);

        System.out.println("\n" + repeatString("=", 70));
        System.out.println("📊 S3 to Kafka Pipeline Application");
        System.out.println(repeatString("=", 70));
        System.out.println("Configuration:");
        System.out.println("  Kafka Broker: " + KAFKA_BROKER);
        System.out.println("  Kafka Topic: " + KAFKA_TOPIC);
        System.out.println("  S3 Path: " + S3_PATH);
        System.out.println("  Processing Delay: " + PROCESSING_DELAY_MS + "ms");

        DataPipeline pipeline = null;

        try {
            // Create Spark Session
            SparkSession spark = SparkSession.builder()
                    .appName(APP_NAME)
                    .master("local[*]")
                    .config("spark.driver.host", "localhost")
                    .getOrCreate();

            // Initialize services
            KafkaProducerService kafkaProducer = new KafkaProducerService(KAFKA_BROKER, KAFKA_TOPIC);
            kafkaProducer.initialize();

            S3Configuration s3Config = new S3Configuration(
                    AWS_ACCESS_KEY,
                    AWS_SECRET_KEY,
                    S3_PATH,
                    S3_ENDPOINT
            );

            S3DataProcessorService s3Processor = new S3DataProcessorService(spark, s3Config);

            // Create and execute pipeline
            pipeline = new DataPipeline(s3Processor, kafkaProducer, PROCESSING_DELAY_MS);
            pipeline.execute();

        } catch (Exception e) {
            System.err.println("\n✗ Application failed!");
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Cleanup
            if (pipeline != null) {
                pipeline.cleanup();
            }
            System.out.println("\n" + repeatString("=", 70));
            System.out.println("Application terminated!");
            System.out.println(repeatString("=", 70));
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
