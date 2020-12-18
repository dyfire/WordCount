package org.myorg.statistic;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.myorg.model.WordBean;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordMapper extends Mapper<LongWritable, Text, Text, WordBean> {
    private String fileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fileName = ((FileSplit) context.getInputSplit())
                .getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());

        while (stringTokenizer.hasMoreTokens()) {
            String word = stringTokenizer.nextToken();
            context.write(new Text(word), new WordBean(word, fileName));
        }
    }
}
