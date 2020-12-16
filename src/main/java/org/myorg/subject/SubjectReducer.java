package org.myorg.subject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.myorg.model.SubjectBean;

import java.io.IOException;

public class SubjectReducer extends Reducer<Text, SubjectBean, LongWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<SubjectBean> values, Context context)
            throws IOException, InterruptedException {
        for (SubjectBean subjectBean : values) {

        }
    }
}
