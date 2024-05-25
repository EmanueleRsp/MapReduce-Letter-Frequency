#!/bin/bash

# Check if the Hadoop cluster is running
if ! hdfs dfs -test -d /; then
    echo "Hadoop cluster is not running"
    exit 1
fi

# Local project directory
parent_dir=$(dirname $(pwd))

# Array of project names
project_names=("letterFrequencyCombiner" "letterFrequencyInMapping")

# Array of num_reducers
num_reducers_values=(1 2 3)

# Array of save_logs values
save_logs_values=(true)

# Array of download_output values
download_output_values=(true)

# Iterate over each combination of values
for project_name in ${project_names[@]}; do

    # Compile the Java code
    cd ${parent_dir}/${project_name}
    mvn clean package

    for num_reducers in ${num_reducers_values[@]}; do
        for save_logs in ${save_logs_values[@]}; do
            for download_output in ${download_output_values[@]}; do
                cd ${parent_dir}/script
                printf "Executing %s with %d reducers, save_logs=%s, download_output=%s\n" $project_name $num_reducers $save_logs $download_output
                ./run.sh $project_name $num_reducers $save_logs $download_output
            done
        done
    done
done