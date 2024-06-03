package it.unipi.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.conf.Configuration;

public class LetterCount
{
    public static class CounterMapper extends Mapper<Object, Text, NullWritable, LongWritable> 
    {
        private Map<NullWritable, LongWritable> map;
        private final static Pattern CHARACTER_PATTERN = Pattern.compile("[a-z]", Pattern.CASE_INSENSITIVE);

        @Override
        public void setup(Context context)
        {
            // Initialize the map
            map = new HashMap<NullWritable, LongWritable>();
        }
        
        @Override
        public void map(Object key, Text value, Context context) 
        {
            // Convert the line to lower case and remove accents
            String line = StringUtils.removeAccents(value.toString()).toLowerCase();

            for (char ch : line.toCharArray()) {
                // Check if the character is a letter
                if (CHARACTER_PATTERN.matcher(String.valueOf(ch)).matches()) {
                    if (map.containsKey(NullWritable.get())) {
                        map.put(NullWritable.get(), new LongWritable(map.get(NullWritable.get()).get() + 1));
                    } else {
                        map.put(NullWritable.get(), new LongWritable(1));
                    }
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            // Cleanup
            for (Map.Entry<NullWritable, LongWritable> entry : map.entrySet()){
                context.write(entry.getKey(), entry.getValue());
            }
        }

    }

    public static class CounterPartitioner extends Partitioner<NullWritable, LongWritable> {
        @Override
        public int getPartition(NullWritable key, LongWritable value, int numReduceTasks) {
            return (int)(Math.random() * numReduceTasks);
        }
    }

    public static class CounterReducer extends Reducer<NullWritable, LongWritable, NullWritable, LongWritable>
    {
        // Variables
        private final static NullWritable reducerKey = NullWritable.get();
        private LongWritable reducerValue = new LongWritable();

        @Override
        public void reduce(NullWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException 
        {

            // Iterate over the values
            for (LongWritable value : values) {
                reducerValue.set(reducerValue.get() + value.get());
            }

            // Write the output
            context.write(reducerKey, reducerValue);
         
        }
    }

    // Configure the job
    public static Job configureJob(Configuration conf, Map<String, String> argMap, int numReducerTasks) throws IOException {
        Job letterCountJob = Job.getInstance(conf, "Letter Count");
    
        // Set the main classes
        letterCountJob.setJarByClass(LetterCount.class);
        letterCountJob.setMapperClass(CounterMapper.class);
        letterCountJob.setReducerClass(CounterReducer.class);

        // Set the partitioner
        letterCountJob.setPartitionerClass(CounterPartitioner.class);
    
        // Set number of reducers 
        if (argMap.containsKey("numReducers")) {
            letterCountJob.setNumReduceTasks(Integer.parseInt(argMap.get("numReducers")));
        } else {
            letterCountJob.setNumReduceTasks(numReducerTasks);    
        }

        // Set the output key and value classes for the mapper
        letterCountJob.setMapOutputKeyClass(NullWritable.class);
        letterCountJob.setMapOutputValueClass(LongWritable.class);
    
        // Set the output key and value classes for the reducer
        letterCountJob.setOutputKeyClass(NullWritable.class);
        letterCountJob.setOutputValueClass(LongWritable.class);
    
        // Set the input and output paths
        FileInputFormat.addInputPath(letterCountJob, new Path(argMap.get("input")));
        FileOutputFormat.setOutputPath(letterCountJob, new Path(argMap.get("letterCountOutput")));
    
        // Set the input and output formats
        letterCountJob.setInputFormatClass(TextInputFormat.class);
        letterCountJob.setOutputFormatClass(TextOutputFormat.class);
    
        return letterCountJob;
    }

}
