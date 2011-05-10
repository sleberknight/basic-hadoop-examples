package com.nearinfinity.hadoop.patent;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FindCitingPatentsCount extends Configured implements Tool {

    public static enum Counters {
        TOTAL_CITATIONS,
        TOTAL_PATENTS
    }

    // Map inputs: (citing patent, cited patent)
    // Map outputs: (cited patent, citing patent)
    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        private Text citing = new Text();
        private Text cited = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] split = value.toString().split(",");
            citing.set(split[0]);
            cited.set(split[1]);

            context.write(cited, citing);
            context.getCounter(Counters.TOTAL_CITATIONS).increment(1L);
        }
    }

    // Reduce inputs: (cited patent, list(citing patent))
    // Reduce outputs: (cited patent, count of citing patents)
    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {

        private IntWritable citingCount = new IntWritable();

        @SuppressWarnings({ "UnusedDeclaration" })
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            for (Text value : values) {
                count++;
            }

            citingCount.set(count);
            context.write(key, citingCount);
            context.getCounter(Counters.TOTAL_PATENTS).increment(1L);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = new Job(conf, FindCitingPatentsCount.class.getSimpleName());
        job.setJarByClass(FindCitingPatentsCount.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new FindCitingPatentsCount(), args);
        System.exit(result);
    }

}
