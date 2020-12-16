package org.myorg.subject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.myorg.Tool;
import org.myorg.model.SubjectBean;

import javax.xml.soap.Text;
import java.io.File;
import java.io.IOException;

public class SubjectRun {
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        Tool.deleteDir(new File("output"));

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(SubjectRun.class);

        job.setMapperClass(SubjectMapper.class);
        job.setReducerClass(SubjectReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SubjectBean.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ret = job.waitForCompletion(true);

        System.exit(ret ? 1 : 0);
    }
}
