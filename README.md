# Spark JSON Data Sender Application

A Java application that reads JSON stub files and processes them using Apache Spark, sending data one by one.

## Prerequisites

- Java 8 or higher
- Gradle 7.6+ (included via wrapper)
- Apache Spark 2.4.4 (installed at `/usr/local/spark/spark-2.4.4-bin-hadoop2.6`)

## Project Structure

```
Spark/
├── build.gradle                     # Gradle build configuration
├── settings.gradle                  # Gradle settings
├── gradlew                          # Gradle wrapper (Unix)
├── gradle/
│   └── wrapper/                    # Gradle wrapper files
├── run.sh                           # Build and run script
├── data/
│   └── sample-data.json            # Sample JSON stub file
└── src/
    └── main/
        └── java/
            └── com/
                └── poc/
                    └── spark/
                        └── JsonDataSender.java  # Main application
```

## Features

- **Reads JSON files** using Apache Spark SQL
- **Processes data one by one** with configurable delay
- **Two processing methods**:
  - Method 1: Spark SQL DataFrames
  - Method 2: Manual JSON parsing with RDDs
- **Extensible** - Easy to add your own data sending logic

## Sample JSON Format

The application expects JSON files with the following structure:

```json
[
  {
    "id": 1,
    "name": "John Doe",
    "email": "john.doe@example.com",
    "age": 30,
    "department": "Engineering"
  }
]
```

## Building the Application

```bash
./gradlew clean shadowJar
```

## Running the Application

### Option 1: Using the run script (Recommended)

```bash
chmod +x run.sh
./run.sh
```

To use a custom JSON file:

```bash
./run.sh path/to/your/data.json
```

### Option 2: Using spark-submit directly

```bash
export SPARK_HOME=/usr/local/spark/spark-2.4.4-bin-hadoop2.6

$SPARK_HOME/bin/spark-submit \
    --class com.poc.spark.JsonDataSender \
    --master local[*] \
    --driver-memory 2g \
    build/libs/spark-json-reader-1.0-SNAPSHOT.jar \
    data/sample-data.json
```

### Option 3: Using Gradle run task (for development)

```bash
./gradlew run --args="data/sample-data.json"
```

## How It Works

1. **Reads JSON file** - The application reads the specified JSON file using Spark
2. **Parses data** - Converts JSON into structured data (DataFrame or Java objects)
3. **Processes one by one** - Iterates through each record sequentially
4. **Sends data** - Calls the `sendData()` method for each record (customize this for your needs)
5. **Adds delay** - 500ms delay between records to simulate real-time processing

## Customization

### Modify the send logic

Edit the `sendData()` and `sendDataAsJson()` methods in [JsonDataSender.java](src/main/java/com/poc/spark/JsonDataSender.java) to implement your actual data sending logic:

```java
private static void sendData(Row row) {
    // Add your logic here:
    // - Send to Kafka
    // - HTTP POST to API
    // - Write to database
    // - etc.
}
```

### Change the delay

Modify the `Thread.sleep(500)` value (in milliseconds) to adjust the delay between records.

### Add more fields

Update the `Employee` class to match your JSON structure.

## Output Example

```
=================================================
Spark JSON Data Sender Application
=================================================
Reading JSON file: data/sample-data.json

--- Method 1: Using Spark SQL ---
Total records: 5

[Record 1]
  ID: 1
  Name: John Doe
  Email: john.doe@example.com
  Age: 30
  Department: Engineering
  >> Data sent successfully!
...
```

## Troubleshooting

### Gradle wrapper not executable
Make the wrapper executable:
```bash
chmod +x gradlew
```

### Spark not found
Verify Spark installation:
```bash
ls -la /usr/local/spark/spark-2.4.4-bin-hadoop2.6
```

### Build fails
Ensure Java is installed:
```bash
java -version
```

## License

This is a POC (Proof of Concept) application.
