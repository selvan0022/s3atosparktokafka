package com.spark.test.pipeline;

import com.spark.test.kafka.KafkaProducerService;
import com.spark.test.spark.S3DataProcessorService;
import org.apache.spark.sql.Row;

import java.util.List;

/**
 * Pipeline orchestrator for coordinating S3 -> Spark -> Kafka flow
 */
public class DataPipeline {
    private final S3DataProcessorService s3Processor;
    private final KafkaProducerService kafkaProducer;
    private final long processingDelayMs;

    /**
     * Constructor
     * @param s3Processor S3 data processor service
     * @param kafkaProducer Kafka producer service
     * @param processingDelayMs Delay between processing records (milliseconds)
     */
    public DataPipeline(S3DataProcessorService s3Processor, KafkaProducerService kafkaProducer, long processingDelayMs) {
        this.s3Processor = s3Processor;
        this.kafkaProducer = kafkaProducer;
        this.processingDelayMs = processingDelayMs;
    }

    /**
     * Execute the complete pipeline: Read from S3 -> Process with Spark -> Send to Kafka
     */
    public void execute() {
        try {
            System.out.println("\n" + repeatString("=", 60));
            System.out.println("🚀 Starting S3 -> Spark -> Kafka Pipeline");
            System.out.println(repeatString("=", 60));

            // Step 1: Read from S3
            org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> dataset = s3Processor.readFromS3();

            // Step 2: Validate data
            org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> validRecords = s3Processor.validateData(dataset);

            // Step 3: Process records
            List<org.apache.spark.sql.Row> records = s3Processor.processRecords(validRecords);

            // Step 4: Send to Kafka
            sendRecordsToKafka(records);

            System.out.println("\n" + repeatString("=", 60));
            System.out.println("✓ Pipeline execution completed successfully!");
            System.out.println(repeatString("=", 60));

        } catch (Exception e) {
            System.err.println("\n✗ Pipeline execution failed!");
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Send all records to Kafka
     * @param records List of Row objects to send
     */
    private void sendRecordsToKafka(List<Row> records) {
        System.out.println("\n📨 Sending records to Kafka...");
        System.out.println("  Total records to send: " + records.size());
        System.out.println(repeatString("--", 30));

        int counter = 1;
        for (Row row : records) {
            try {
                System.out.println("\n[Record " + counter + " / " + records.size() + "]");
                printRowDetails(row);

                // Convert row to JSON and send to Kafka
                String key = getRowKey(row);
                String jsonValue = s3Processor.rowToJson(row);

                kafkaProducer.sendMessage(key, jsonValue);

                counter++;

                // Add delay between records
                if (processingDelayMs > 0) {
                    Thread.sleep(processingDelayMs);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("✗ Processing interrupted: " + e.getMessage());
                break;
            } catch (Exception e) {
                System.err.println("✗ Error processing record " + counter + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        System.out.println("\n" + repeatString("--", 30));
        System.out.println("✓ Sent " + (counter - 1) + " records to Kafka");
    }

    /**
     * Get a unique key for the row (from 'id' column if available)
     * @param row Row object
     * @return Unique key for the row
     */
    private String getRowKey(Row row) {
        try {
            Object idValue = row.getAs("id");
            if (idValue != null) {
                return idValue.toString();
            }
        } catch (Exception e) {
            // ID column doesn't exist or is null
        }
        return "key-" + System.currentTimeMillis();
    }

    /**
     * Print row details for logging
     * @param row Row object
     */
    private void printRowDetails(Row row) {
        String[] columns = row.schema().fieldNames();
        for (String col : columns) {
            try {
                Object value = row.getAs(col);
                System.out.println("  " + col + ": " + value);
            } catch (Exception e) {
                System.out.println("  " + col + ": <error reading>");
            }
        }
    }

    /**
     * Cleanup resources
     */
    public void cleanup() {
        System.out.println("\n🧹 Cleaning up resources...");
        kafkaProducer.close();
        s3Processor.close();
        System.out.println("✓ Cleanup completed!");
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
