package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.w3c.dom.css.Counter;

import it.unipi.hadoop.LetterCount;
import it.unipi.hadoop.LetterCount.CounterMapper;
import it.unipi.hadoop.LetterCount.CounterReducer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;


public class LetterFrequency
{

    public static class FrequencyMapper extends Mapper<Object, Text, Text, IntWritable> 
    {
        private Text reducerKey = new Text();
        private final static IntWritable reducerValue = new IntWritable(1);

        public void setup(Context context)
        {
            // Configuration
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {   
            // Convert the line to lower case
            String line = value.toString().toLowerCase();

            for (char ch : line.toCharArray()) {
                // Check if the character is a letter
                if (Character.isLetter(ch)) { 
                    reducerKey.set(String.valueOf(ch));
                    context.write(reducerKey, reducerValue);
                }
            }
        }
    }

    public static class FrequencyCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            // Variables
            int sum = 0;

            // Iterate over the values
            for (IntWritable value : values) {
                sum += value.get();
            }

            // Write the output
            context.write(key, new IntWritable(sum));
        }
    }

    public static class FrequencyReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>
    {
        // Variables
        private static int TEXT_LENGTH;

        public void setup(Context context)
        {
            // Configuration
            Configuration conf = context.getConfiguration();
            TEXT_LENGTH = Integer.parseInt(conf.get("textLength"));
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            // Variables
            int sum = 0;

            // Iterate over the values
            for (IntWritable value : values) {
                sum += value.get();
            }

            // Write the output
            context.write(key, new DoubleWritable((double)sum / (double)TEXT_LENGTH));
         
        }
    }

    public static void main(String[] args) throws Exception
    {

        // Default values
        final int DEFAULT_NUM_REDUCERS = 1;

        // Configuration of the job
        Configuration conf = new Configuration();

        Map<String, String> argMap = new HashMap<>();
        for (String arg : args) {
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

        // Define the first job
        Job letterCountJob = Job.getInstance(conf, "Letter Count");
        
        // Set the main classes
        letterCountJob.setJarByClass(LetterCount.class);
        letterCountJob.setMapperClass(CounterMapper.class);
        letterCountJob.setReducerClass(CounterReducer.class);
        // Set the combiner class
        letterCountJob.setCombinerClass(CounterReducer.class);
        

        // Set number of reducers 
        if (argMap.containsKey("numReducers")) {
            letterCountJob.setNumReduceTasks(Integer.parseInt(argMap.get("numReducers")));
        }else{
            letterCountJob.setNumReduceTasks(DEFAULT_NUM_REDUCERS);
        }
    
        // Set the output key and value classes for the mapper
        letterCountJob.setMapOutputKeyClass(NullWritable.class);
        letterCountJob.setMapOutputValueClass(IntWritable.class);

        // Set the output key and value classes for the reducer
        letterCountJob.setOutputKeyClass(NullWritable.class);
        letterCountJob.setOutputValueClass(IntWritable.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(letterCountJob, new Path(argMap.get("input")));
        FileOutputFormat.setOutputPath(letterCountJob, new Path(argMap.get("letterCountOutput")));

        // Set the input and output formats
        letterCountJob.setInputFormatClass(TextInputFormat.class);
        letterCountJob.setOutputFormatClass(TextOutputFormat.class);


        // Wait for the first job to complete
        if (!letterCountJob.waitForCompletion(true)) {
            System.exit(1);
        }

        // Read the output of the first job
        FileSystem fs = FileSystem.get(conf);
        String directory = argMap.get("letterCountOutput");
        String filePath = directory + "/part-r-00000";
        org.apache.hadoop.fs.Path outputPath = new org.apache.hadoop.fs.Path(filePath);        FSDataInputStream inputStream = fs.open(outputPath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        // Assume the number you need is on the first line of the output
        String firstLine = bufferedReader.readLine();
        int textLength = Integer.parseInt(firstLine);

        // Close the input stream
        bufferedReader.close();
        inputStream.close();

        // Display the text length
        System.out.println("Letter Count - Text length: " + textLength);
        
        // Create a new Job
        Job letterFrequencyJob = Job.getInstance(conf, "LetterFrequency");

        // Set the configuration
        letterFrequencyJob.getConfiguration().setInt("textLength", textLength);

        // Set the main classes
        letterFrequencyJob.setJarByClass(LetterFrequency.class);
        letterFrequencyJob.setMapperClass(FrequencyMapper.class);
        letterFrequencyJob.setReducerClass(FrequencyReducer.class);

        // Set the combiner class
        letterFrequencyJob.setCombinerClass(FrequencyCombiner.class);

        // Set number of reducers 
        if (argMap.containsKey("numReducers")) {
            letterFrequencyJob.setNumReduceTasks(Integer.parseInt(argMap.get("numReducers")));
        }else{
            letterFrequencyJob.setNumReduceTasks(DEFAULT_NUM_REDUCERS);
        }

        // Set the output key and value classes for the mapper
        letterFrequencyJob.setMapOutputKeyClass(Text.class);
        letterFrequencyJob.setMapOutputValueClass(IntWritable.class);
    
        // Set the output key and value classes for the reducer
        letterFrequencyJob.setOutputKeyClass(Text.class);
        letterFrequencyJob.setOutputValueClass(DoubleWritable.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(letterFrequencyJob, new Path(argMap.get("input")));
        FileOutputFormat.setOutputPath(letterFrequencyJob, new Path(argMap.get("letterFrequencyOutput")));

        // Set the input and output formats
        letterFrequencyJob.setInputFormatClass(TextInputFormat.class);
        letterFrequencyJob.setOutputFormatClass(TextOutputFormat.class);

        // Exit
        System.exit(letterFrequencyJob.waitForCompletion(true) ? 0 : 1);
     }
}
