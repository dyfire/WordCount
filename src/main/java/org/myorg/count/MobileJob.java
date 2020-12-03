package org.myorg.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.myorg.Tool;
import org.myorg.model.FlowBean;

import java.io.File;
import java.io.IOException;

public class MobileJob {
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        Tool.deleteDir(new File("output"));

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MobileJob.class);

        job.setMapperClass(MobileMapper.class);
        job.setReducerClass(MobileReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ret = job.waitForCompletion(true);

        System.exit(ret ? 1 : 0);
    }
}
