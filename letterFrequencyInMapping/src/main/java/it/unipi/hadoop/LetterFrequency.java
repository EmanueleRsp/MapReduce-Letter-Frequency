package it.unipi.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.regex.Pattern;

public class LetterFrequency
{

    public static class FrequencyMapper extends Mapper<Object, Text, Text, LongWritable> 
    {
        private Map<Text, LongWritable> map;
        private final static Pattern CHARACTER_PATTERN = Pattern.compile("[a-z]", Pattern.CASE_INSENSITIVE);

        @Override
        public void setup(Context context)
        {
            // Initialize the map
            map = new HashMap<Text, LongWritable>();
        }

        @Override
        public void map(Object key, Text value, Context context)
        {   
            // Convert the line to lower case and remove accents
            String line = StringUtils.removeAccents(value.toString()).toLowerCase();
        

            for (char ch : line.toCharArray()) {
                // Check if the character is a letter
                if (CHARACTER_PATTERN.matcher(String.valueOf(ch)).matches()) { 
                    if (map.containsKey(new Text(String.valueOf(ch)))) {
                        map.put(new Text(String.valueOf(ch)), new LongWritable(map.get(new Text(String.valueOf(ch))).get() + 1));
                    } else {
                        map.put(new Text(String.valueOf(ch)), new LongWritable(1));
                    }
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            // Write the output
            for (Text key : map.keySet()) {
                context.write(key, map.get(key));
            }
        }
    }

    public static class FrequencyReducer extends Reducer<Text, LongWritable, Text, DoubleWritable>
    {
        // Variables
        private static long TEXT_LENGTH;

        public void setup(Context context)
        {
            // Configuration
            Configuration conf = context.getConfiguration();
            TEXT_LENGTH = Long.parseLong(conf.get("textLength"));
        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException 
        {
            // Variables
            int sum = 0;

            // Iterate over the values
            for (LongWritable value : values) {
                sum += value.get();
            }

            // Write the output
            context.write(key, new DoubleWritable((double)sum / (double)TEXT_LENGTH));
         
        }
    }

    public static Job configureJob(Configuration conf, Map<String, String> argMap, long textLength, int numReducerTasks) throws IOException {
        Job letterFrequencyJob = Job.getInstance(conf, "LetterFrequency");
    
        // Set the configuration
        letterFrequencyJob.getConfiguration().setLong("textLength", textLength);
    
        // Set the main classes
        letterFrequencyJob.setJarByClass(LetterFrequency.class);
        letterFrequencyJob.setMapperClass(FrequencyMapper.class);
        letterFrequencyJob.setReducerClass(FrequencyReducer.class);
    
        // Set number of reducers 
        if (argMap.containsKey("numReducers")) {
            letterFrequencyJob.setNumReduceTasks(Integer.parseInt(argMap.get("numReducers")));
        } else {
            letterFrequencyJob.setNumReduceTasks(numReducerTasks);
        }
    
        // Set the output key and value classes for the mapper
        letterFrequencyJob.setMapOutputKeyClass(Text.class);
        letterFrequencyJob.setMapOutputValueClass(LongWritable.class);
    
        // Set the output key and value classes for the reducer
        letterFrequencyJob.setOutputKeyClass(Text.class);
        letterFrequencyJob.setOutputValueClass(DoubleWritable.class);
    
        // Set the input and output paths
        FileInputFormat.addInputPath(letterFrequencyJob, new Path(argMap.get("input")));
        FileOutputFormat.setOutputPath(letterFrequencyJob, new Path(argMap.get("letterFrequencyOutput")));
    
        // Set the input and output formats
        letterFrequencyJob.setInputFormatClass(TextInputFormat.class);
        letterFrequencyJob.setOutputFormatClass(TextOutputFormat.class);
    
        return letterFrequencyJob;
    }

}
