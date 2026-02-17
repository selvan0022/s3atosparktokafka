#!/bin/bash

# Spark JSON Data Sender - Build and Run Script

echo "================================================="
echo "Building and Running Spark JSON Data Sender"
echo "================================================="

# Set Java 8 for Spark 2.4.4 compatibility
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH

# Set Spark home
export SPARK_HOME=/usr/local/spark/spark-2.4.4-bin-hadoop2.6
export PATH=$SPARK_HOME/bin:$PATH

# Check if Spark exists
if [ ! -d "$SPARK_HOME" ]; then
    echo "ERROR: Spark not found at $SPARK_HOME"
    echo "Please verify Spark installation path"
    exit 1
fi

echo "Spark Home: $SPARK_HOME"
echo "Java Home: $JAVA_HOME"
echo ""

# Build the application
echo "Step 1: Building the application with Gradle..."
/opt/homebrew/Cellar/gradle/9.3.1/bin/gradle --status clean fatJar

echo $JAVA_HOME
if [ $? -ne 0 ]; then
    echo "ERROR: Build failed!"
    exit 1
fi

echo ""
echo "Step 2: Running the application with spark-submit..."
echo ""

# Get the JSON file path (default or from argument)
JSON_FILE="${1:-data/sample-data.json}"

# Run with spark-submit
$SPARK_HOME/bin/spark-submit \
    --class com.poc.spark.JsonDataSender \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    build/libs/spark-json-reader-1.0-SNAPSHOT-all.jar \
    $JSON_FILE

echo ""
echo "================================================="
echo "Execution completed!"
echo "================================================="
