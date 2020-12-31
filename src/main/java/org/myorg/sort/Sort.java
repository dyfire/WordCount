package org.myorg.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.myorg.Tool;

import java.io.File;
import java.io.IOException;

/**
 * 商品id,点击次数
 * 商品点击次数由低到高进行排序
 */
public class Sort {
    public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
        private final LongWritable intWritable = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arr = line.split("\t");
            intWritable.set(Integer.parseInt(arr[1])); // 把要排序的字段装换为LongWriter类型设置为key
            context.write(intWritable, new Text(arr[0])); // 商品ID设置为value,最终输出<key, value>
        }
    }

    /**
     * 数据到达reduce之前,MapReduce框架已经按照key值对这些数据按照键值排序了,shuffle
     * key为封装的int为LongWritable类型，那么MapReduce按照数字大小对key排序
     * Key为封装String的Text类型，那么MapReduce将按照数据字典顺序对字符排序
     * 一般在map中把要排序的字段使用key为封装的int为LongWritable类型，作为key，不排序的字段作为value
     */
    public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(key, text);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Tool.deleteDir(new File("output"));

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(Sort.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ret = job.waitForCompletion(true);

        System.exit(ret ? 1 : 0);
    }
}
