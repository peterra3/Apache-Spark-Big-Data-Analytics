#!/bin/bash

# Check if the script is run on ecehadoop
hostname | grep ecehadoop
if [ $? -eq 1 ]; then
    echo "This script must be run on ecehadoop :("
    exit -1
fi

# Check if the input arguments are provided
if [ $# -ne 3 ]; then
    echo "Usage: $0 <TaskFile.scala> <InputDir> <ExpectedOutputDir>"
    exit 1
fi

SCALA_FILE="$1"
INPUT_FILE="$2"
EXPECTED_OUTPUT_FILE="$3"
CLASS_NAME=$(basename "$SCALA_FILE" .scala)

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export SCALA_HOME=/usr
export HADOOP_HOME=/opt/hadoop-latest/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop/
export SPARK_HOME=/opt/spark-latest/
MAIN_SPARK_JAR=`ls $SPARK_HOME/jars/spark-core*.jar`
export CLASSPATH=".:$MAIN_SPARK_JAR"

echo --- Deleting
rm "${CLASS_NAME}.jar"
rm "${CLASS_NAME}"*.class

echo --- Compiling
$SCALA_HOME/bin/scalac -J-Xmx1g "$SCALA_FILE"
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf "${CLASS_NAME}.jar" "${CLASS_NAME}"*.class

echo --- Running
INPUT=/user/${USER}/a2_inputs/
OUTPUT=/user/${USER}/a2_starter_code_output_spark/

$HADOOP_HOME/bin/hdfs dfs -rm -R $INPUT
$HADOOP_HOME/bin/hdfs dfs -mkdir -p $INPUT
$HADOOP_HOME/bin/hdfs dfs -rm -R $OUTPUT
$HADOOP_HOME/bin/hdfs dfs -copyFromLocal "./input/$INPUT_FILE" $INPUT

echo "Input File: $INPUT_FILE"
echo "Expected Output File: $EXPECTED_OUTPUT_FILE"

# Make temp_output directory
mkdir -p temp_output

RUNTIME="./temp_output/elapsed_time.txt"
DOUTPUT="./temp_output/spark_output.log"

# Execute the Spark job with timeout and time measurement
/usr/bin/time -f "%e" -o $RUNTIME timeout 700 $SPARK_HOME/bin/spark-submit --master yarn --class "$CLASS_NAME" --driver-memory 4g --executor-memory 4g "${CLASS_NAME}.jar" $INPUT $OUTPUT > $DOUTPUT 2> /dev/null
if [ $? -ne 0 ]; then
    echo "Error: Spark job failed or timed out."
    if [ -f "$RUNTIME" ]; then
        elapsed_time=$(cat "$RUNTIME")
        echo "Elapsed Time: $elapsed_time seconds."
    fi
    exit 2
fi

elapsed_time=$(cat "$RUNTIME")
echo "Elapsed Time: $elapsed_time seconds."

export HADOOP_ROOT_LOGGER="WARN"

# $HADOOP_HOME/bin/hdfs dfs -ls $OUTPUT
# $HADOOP_HOME/bin/hdfs dfs -cat $OUTPUT/*

$HADOOP_HOME/bin/hdfs dfs -cat $OUTPUT/* | sort > ./temp_output/normalized_output_a.txt
cat "./output/$EXPECTED_OUTPUT_FILE" | sort > ./temp_output/normalized_output_b.txt

if diff ./temp_output/normalized_output_a.txt ./temp_output/normalized_output_b.txt > /dev/null; then
    echo "The output is as expected."
    exit 0
else
    echo "The output is different from the expected output."
    exit 2
fi