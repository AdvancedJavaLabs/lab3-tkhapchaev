package ru.sales.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.println("Usage: ru.sales.mapreduce.SalesJob <input_path> <output_path> <reduce_task_count> <map_thread_count> <input_file_split_min_size> <input_file_split_max_size>");
            System.exit(-1);
        }

        int reduceTaskCount = Integer.parseInt(args[2]);
        int mapThreadCount = Integer.parseInt(args[3]);
        long inputFileSplitMinSize = Long.parseLong(args[4]);
        long inputFileSplitMaxSize = Long.parseLong(args[5]);

        Configuration configuration = new Configuration();

        configuration.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        configuration.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

        configuration.set("mapreduce.input.fileinputformat.split.minsize", String.valueOf(inputFileSplitMinSize));
        configuration.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(inputFileSplitMaxSize));

        Job job = Job.getInstance(configuration, "Sales per category with sorting (" + args[2] + ", " + args[3] + ")");
        job.setJarByClass(SalesJob.class);

        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(configuration);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        long startTime = System.currentTimeMillis();

        job.setMapperClass(MultithreadedMapper.class);

        MultithreadedMapper.setMapperClass(job, SalesMapper.class);
        MultithreadedMapper.setNumberOfThreads(job, mapThreadCount);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SalesWritable.class);

        job.setReducerClass(SalesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(reduceTaskCount);

        job.setInputFormatClass(CombineTextInputFormat.class);

        CombineTextInputFormat.setMinInputSplitSize(job, inputFileSplitMinSize);
        CombineTextInputFormat.setMaxInputSplitSize(job, inputFileSplitMaxSize);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        long elapsedMilliseconds = endTime - startTime;
        double elapsedSeconds = elapsedMilliseconds / 1000.0;

        System.out.println("\n================ Job statistics ================");
        System.out.println("Job ID: " + job.getJobID());
        System.out.println("Job name: " + job.getJobName());
        System.out.println("Status: " + (status ? "SUCCESS" : "FAILED"));
        System.out.println("Number of reduce tasks: " + reduceTaskCount);
        System.out.println("Number of map threads: " + mapThreadCount);
        System.out.println("Input file split min size: " + inputFileSplitMinSize + " bytes");
        System.out.println("Input file split max size: " + inputFileSplitMaxSize + " bytes");
        System.out.println("Execution time: " + elapsedMilliseconds + " ms (" + elapsedSeconds + " s)");
        System.out.println("================================================\n");

        System.exit(status ? 0 : 1);
    }
}