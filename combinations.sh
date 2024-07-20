#!/bin/bash

# Input and task files range
INPUT_FILES=("in0.txt" "in1.txt" "in2.txt" "in3.txt" "in4.txt" "in5.txt")
TASK_FILES=("Task1.scala" "Task2.scala" "Task3.scala" "Task4.scala")
function display_elapsed_time {
    if [ -f "./temp_output/elapsed_time.txt" ]; then
        elapsed_time=$(cat "./temp_output/elapsed_time.txt")
        echo "Elapsed Time: $elapsed_time seconds."
    fi
}

# Iterate over all combinations of task files and input files
for task in "${TASK_FILES[@]}"; do
    for input in "${INPUT_FILES[@]}"; do
        # Extract task and input indices
        task_idx="${task%.scala}" # Removes .scala
        input_idx="${input%.txt}" # Removes .txt

        # Expected output file
        expected_output_file="t${task_idx#Task}_$input"

        echo "Starting for Task $task_idx and Input $input_idx..."

        # Run the buildrun_wc_spark_cluster.sh script and suppress its output
        bash ./buildrun_wc_spark_cluster.sh "$task" "$input" "$expected_output_file" > /dev/null 2>&1


        # Check the result of the script
        if [ $? -eq 0 ]; then
            echo "Task $task_idx and Input $input_idx completed successfully."
            display_elapsed_time
        else
            echo "Task $task_idx and Input $input_idx failed."
            display_elapsed_time
        fi
    done
done
