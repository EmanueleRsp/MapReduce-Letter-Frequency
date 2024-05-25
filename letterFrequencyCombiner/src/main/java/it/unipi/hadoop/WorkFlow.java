package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class WorkFlow {

    // Default values
    final static int DEFAULT_NUM_REDUCERS = 1;

    public static Map<String, String> parseArguments(String[] args) {
        Map<String, String> argMap = new HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("it.unipi.hadoop")) {
                continue;
            }
            String[] parts = arg.split("=");
            if (parts.length == 2) {
                argMap.put(parts[0], parts[1]);
            } else {
                System.err.println("Invalid argument: " + arg);
                System.exit(1);
            }
        }
    
        if (!argMap.containsKey("input") || !argMap.containsKey("letterCountOutput") || !argMap.containsKey("letterFrequencyOutput")) {
            System.err.println("Usage: LetterFrequency input=<input> letterCountOutput=<output> letterFrequencyOutput=<output> [numReducers=<num of reducer tasks>]");
            System.exit(1);
        }

        System.out.println("args[0]: <input>="  + argMap.get("input"));
        System.out.println("args[1]: <letterCountOutput>=" + argMap.get("letterCountOutput"));
        System.out.println("args[2]: <letterFrequencyOutput>=" + argMap.get("letterFrequencyOutput"));
    
        return argMap;
    }

    public static int readTextLength(Configuration conf, String outputDirectory) throws IOException {
        // Read the output of the first job
        FileSystem fs = FileSystem.get(conf);
        String filePath = outputDirectory + "/part-r-00000";
        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(filePath);
        FSDataInputStream inputStream = fs.open(outputPath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        // The result is on the first line of the output
        String firstLine = bufferedReader.readLine();
        int textLength = Integer.parseInt(firstLine);

        // Close the input stream
        bufferedReader.close();
        inputStream.close();

        // Display the text length
        System.out.println("Letter Count - Text length: " + textLength);

        return textLength;
    }
    
    public static void main(String[] args) throws Exception
    {

        // Configuration of the job
        Configuration conf = new Configuration();

        // Parse the arguments
        Map<String, String> argMap = parseArguments(args);

        // Create a letter count job
        Job letterCountJob = LetterCount.configureJob(conf, argMap, DEFAULT_NUM_REDUCERS);
        // Wait for the first job to complete
        if (!letterCountJob.waitForCompletion(true)) {
            System.exit(1);
        }

        // Read the text length
        int textLength = readTextLength(conf, argMap.get("letterCountOutput"));
        
        // Create a letter frequency job
        Job letterFrequencyJob = LetterFrequency.configureJob(conf, argMap, textLength, DEFAULT_NUM_REDUCERS);
        // Wait for the second job to complete
        System.exit(letterFrequencyJob.waitForCompletion(true) ? 0 : 1);

     }
}
