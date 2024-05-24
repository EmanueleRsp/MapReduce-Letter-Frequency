#!/bin/bash

set -e

# Local project directory
parent_dir=$(dirname $(pwd))

# Compile the Java code
cd ${parent_dir}/letterFrequency
mvn clean package

# Check if the Hadoop cluster is running
if ! hdfs dfs -test -d /; then
    echo "Hadoop cluster is not running"
    exit 1
fi

# Upload the JAR file to the Hadoop cluster
cd ${parent_dir}/letterFrequency
scp target/letterFrequency-1.0-SNAPSHOT.jar  hadoop@10.1.1.77:

# Create a directory in HDFS to store the project files
project_dir=/user/$(whoami)/letterFrequency
hdfs dfs -mkdir -p ${project_dir}
printf "HDFS Project directory %s\n" ${project_dir}

# Create a directory in HDFS to store the input file
input_dir=${project_dir}/input
hdfs dfs -mkdir -p ${input_dir}
printf "HDFS Input directory %s\n" ${input_dir}

# Create a directory in HDFS to store the output files
output_dir=${project_dir}/output
hdfs dfs -mkdir -p ${output_dir}
printf "HDFS Output directory %s\n" ${output_dir}


# Upload each input file to HDFS if it does not exist
cd ${parent_dir}
for file in resources/input/*; do
    filename=$(basename $file)
    if ! hdfs dfs -test -e ${input_dir}/${filename}; then
        hdfs dfs -put $file ${input_dir}
    fi
    printf "HDFS Input file %s\n" ${input_dir}/${filename}
done


# Read the current run number from the file, if it exists.
cd ${parent_dir}/script
if [ -f "run_number.txt" ]; then
    run_number=$(cat run_number.txt)
else
    run_number=0
fi
# Format the run number as a 5-digit number
formatted_number=$(printf "%05d" $run_number)
printf "Execution number %s\n" $formatted_number


# Execute Hadoop WorkFlow for each input
cd ${parent_dir}
for file in resources/input/*; do
    # Get the filename
    filename=$(basename $file .txt)

    # Execute the Hadoop WorkFlow
    cd ${parent_dir}/letterFrequency
    hadoop jar target/letterFrequency-1.0-SNAPSHOT.jar \
    it.unipi.hadoop.WorkFlow \
    input=${input_dir}/${filename}.txt \
    letterCountOutput=${output_dir}/output_${formatted_number}/${filename}/count \
    letterFrequencyOutput=${output_dir}/output_${formatted_number}/${filename}/frequency

done


# Increment the run number and save it to the file
cd ${parent_dir}/script
echo $((run_number + 1)) > run_number.txt


# Download the output files from HDFS
cd ${parent_dir}
for file in resources/input/*; do
    # Get the filename
    filename=$(basename $file .txt)
    
    mkdir -p resources/output/output_${formatted_number}/${filename}/count
    hdfs dfs -get ${output_dir}/output_${formatted_number}/${filename}/count/part-r-00000 \
    resources/output/output_${formatted_number}/${filename}/count
    mkdir -p resources/output/output_${formatted_number}/${filename}/frequency
    hdfs dfs -get ${output_dir}/output_${formatted_number}/${filename}/frequency/part-r-00000 \
    resources/output/output_${formatted_number}/${filename}/frequency
    printf "Output files downloaded\n"

    # Print the output file
    printf "Text length: "
    cat resources/output/output_${formatted_number}/${filename}/count/part-r-00000
    printf "Letter frequency:\n"
    cat resources/output/output_${formatted_number}/${filename}/frequency/part-r-00000
done