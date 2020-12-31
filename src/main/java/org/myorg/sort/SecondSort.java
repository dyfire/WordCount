package org.myorg.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.myorg.Tool;
import org.myorg.model.SortBean;

import java.io.File;
import java.io.IOException;

public class SecondSort {
    /**
     * 二次排序Map类
     * 输入类型为(LongWritable,Text)
     * 输出类型为(SortBean,NullWritable)
     */
    public static class Map extends Mapper<LongWritable, Text, SortBean, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            SortBean bean = new SortBean();
            bean.setFirst(Long.parseLong(line[0]));
            bean.setSecond(Long.parseLong(line[1]));

            context.write(bean, NullWritable.get());
        }
    }

    /**
     * 输入值(SortBean,NullWritable)
     * 输出值(IntWritable,Text)
     */
    public static class Reduce extends Reducer<SortBean, NullWritable, LongWritable, Text> {
        @Override
        protected void reduce(SortBean key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            for (NullWritable nullWritable : values) {
                context.write(new LongWritable(key.getFirst()),
                        new Text(String.valueOf(key.getSecond())));
            }
        }
    }

    /**
     * 自定义分区,同一个商品ID的key进入同一个分区
     */
    public static class Partition extends Partitioner<SortBean, NullWritable> {
        @Override
        public int getPartition(SortBean sortBean, NullWritable number, int i) {
            return (int) (sortBean.getFirst()) % i;
        }
    }

    /**
     * 排序对比类
     * 实现key的比较器，在定义key中已经实现compareTo方法
     */
    public static class Comparator extends WritableComparator {
        /**
         * 通过构造方法传递key值
         */
        protected Comparator() {
            super(SortBean.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            SortBean k1 = (SortBean) a;
            SortBean k2 = (SortBean) b;

            return k1.compareTo(k2);
        }
    }

    /**
     * 分组对比类,按照商品ID进行分组，同一个分组进入同一个reduce
     */
    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(SortBean.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            SortBean k1 = (SortBean) a;
            SortBean k2 = (SortBean) b;

            return (int) (k1.getFirst()) - (int) (k2.getSecond());
        }
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        Tool.deleteDir(new File("output"));

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 设置作业的执行类
        job.setJarByClass(SecondSort.class);

        // map类
        job.setMapperClass(SecondSort.Map.class);

        // reduce类
        job.setReducerClass(SecondSort.Reduce.class);

        // map输出key数据类型
        job.setMapOutputKeyClass(SortBean.class);

        // map输出value数据类型
        job.setMapOutputValueClass(NullWritable.class);

        // Reduce个数
        job.setNumReduceTasks(3);

        // 分区类
        job.setPartitionerClass(Partition.class);

        // 排序对比类
        job.setSortComparatorClass(Comparator.class);

        // 分组对比类
        job.setGroupingComparatorClass(GroupComparator.class);

        // 数据源路径
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 等待作业启动
        boolean ret = job.waitForCompletion(true);

        System.exit(ret ? 1 : 0);
    }
}
