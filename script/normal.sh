#!/bin/bash

# Check if the Hadoop cluster is running
if ! hdfs dfs -test -d /; then
    echo "Hadoop cluster is not running"
    exit 1
fi

# Local project directory
parent_dir=$(dirname $(pwd))

# project names
project_name="letterFrequencyInMapping"

# num_reducers
num_reducers_value=3

# performance
performance=false

# Compile the Java code
cd ${parent_dir}/${project_name}
mvn clean package

# Execute the project with the specified number of reducers
cd ${parent_dir}/script
printf "Executing %s with %d reducers, performance=%s\n" $project_name $num_reducers_value $performance
./run.sh $project_name $num_reducers_value $performance

printf "All combinations executed\n"
