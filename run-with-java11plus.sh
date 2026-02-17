#!/bin/bash

# Spark JSON Data Sender - Build and Run Script (Java 11+)

echo "================================================="
echo "Building and Running Spark JSON Data Sender"
echo "================================================="

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
/opt/homebrew/Cellar/gradle/9.3.1/bin/gradle clean fatJar

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

# Java 9+ module arguments for Spark 2.4.4 compatibility
JAVA_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

# Run with spark-submit
$SPARK_HOME/bin/spark-submit \
    --class com.poc.spark.JsonDataSender \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    --driver-java-options "$JAVA_OPTS" \
    --conf "spark.executor.extraJavaOptions=$JAVA_OPTS" \
    build/libs/spark-json-reader-1.0-SNAPSHOT-all.jar \
    $JSON_FILE

echo ""
echo "================================================="
echo "Execution completed!"
echo "================================================="
