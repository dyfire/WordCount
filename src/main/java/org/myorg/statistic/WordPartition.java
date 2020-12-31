package org.myorg.statistic;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 全排序：不仅在每个分区上是有序的，它所有的分区按大小合并起来也是有序的
 * 全排序两种方式：1、定义一个reduce;2、自定义分区；3、采样器
 */
public class WordPartition extends Partitioner<IntWritable, IntWritable> {
    @Override
    public int getPartition(IntWritable intWritable, IntWritable intWritable2, int i) {
        return 0;
    }
}
