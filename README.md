
# Big Data Analytics with Apache Spark

## Overview
This project involves performing data analytics on a movie dataset using Apache Spark with Scala. The goal is to implement several tasks as part of a big data analytics pipeline, leveraging Hadoop and Spark for distributed computing.

## Objectives
1. Gain hands-on experience with Apache Spark and Scala.
2. Learn about big data processing and analytics.

## Software Environment
- **Java**: 1.8
- **Hadoop**: 3.3.4
- **Scala**: 2.12
- **Spark**: 3.4.0

## Setup and Build

### Prerequisites
- Java 1.8
- Hadoop 3.3.4
- Scala 2.12
- Spark 3.4.0

### Setup
1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/big-data-analytics.git
   cd big-data-analytics
   ```

2. Ensure Hadoop and Spark are properly configured on your system.

### Building the Project
Compile the Scala code using sbt (Scala Build Tool):
```bash
sbt compile
```

## Running the System
1. Upload sample inputs to HDFS:
   ```bash
   hdfs dfs -put /local/path/to/sample_inputs /a2_inputs
   ```

2. Submit the Spark job:
   ```bash
   spark-submit --class com.example.MovieAnalytics target/scala-2.12/big-data-analytics_2.12-0.1.jar
   ```

## License
This project is licensed under the MIT License.

## Contact
For questions or issues, please contact me via GitHub.

