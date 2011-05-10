package com.nearinfinity.hadoop.wordcount;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BetterWordCount {

    private static final String SKIP_PATTERNS_FILE = "skip.patterns.file";

    public static class MapClass extends Mapper<Object, Text, Text, IntWritable> {

        private static final String CASE_SENSITIVE = "case.sensitive";

        static enum MapCounters {
            INPUT_WORDS
        }

        private boolean caseSensitive = true;
        private Set<String> patternsToSkip = new HashSet<String>();

        private long numLinesProcessed = 0;
        private String mapInputFileName;

        private static final IntWritable ONE = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            // Doesn't work with new API. See http://issues.apache.org/jira/browse/HADOOP-5973
//            mapInputFileName = context.getConfiguration().get("map.input.file");

            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            mapInputFileName = inputSplit.getPath().toString();

            Configuration conf = context.getConfiguration();

            caseSensitive = conf.getBoolean(CASE_SENSITIVE, false);

            if (conf.get(SKIP_PATTERNS_FILE) != null) {
                Path[] patternsFiles = DistributedCache.getLocalCacheFiles(conf);
                System.out.println("patterns files length: " + patternsFiles.length);
                System.out.println("patterns files [0]: " + patternsFiles[0].toUri());
                addSkipPatternsFrom(patternsFiles[0]);
            }
            System.out.printf("Patterns to skip: %s", patternsToSkip.toString());
        }

        private void addSkipPatternsFrom(Path patternsFile) throws IOException {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(patternsFile.toString()));
                String pattern;
                while ((pattern = reader.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            }
            finally {
                if (reader != null) reader.close();
            }
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = caseSensitive ? value.toString() : value.toString().toLowerCase();

            for (String patternToSkip : patternsToSkip) {
                line = line.replaceAll(patternToSkip, "");
            }

            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                word.set(token);
                context.write(word, ONE);
                context.getCounter(MapCounters.INPUT_WORDS).increment(1L);
            }

            if (++numLinesProcessed % 100 == 0) {
                context.setStatus(numLinesProcessed + " lines processed from: " + mapInputFileName);
            }
        }

    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        static enum ReduceCounters {
            UNIQUE_WORDS_COUNTED
        }

        private IntWritable count = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            count.set(sum);
            context.write(key, count);
            context.getCounter(ReduceCounters.UNIQUE_WORDS_COUNTED).increment(1L);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.out.println("Usage: <config options> <input_path> <output_path_noexist>");
            System.out.println("<config options>:");
            System.out.println("  -D case.sensitive=true|false");
            System.out.println("  -D skip.patterns.file=<HDFS_path_to_file_containing_patterns_to_skip>");
            System.exit(1);
        }

        String patternsFilePath = conf.get(SKIP_PATTERNS_FILE);
        if (patternsFilePath != null) {
            System.out.println("patternsFilePath URI = " + new Path(patternsFilePath).toUri());
            DistributedCache.addCacheFile(new Path(patternsFilePath).toUri(), conf);
        }

        Job job = new Job(conf, BetterWordCount.class.getSimpleName());
        job.setJarByClass(BetterWordCount.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
