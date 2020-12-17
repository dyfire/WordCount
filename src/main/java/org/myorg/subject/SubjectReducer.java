package org.myorg.subject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.myorg.model.SubjectBean;

import java.io.IOException;

public class SubjectReducer extends Reducer<Text, SubjectBean, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<SubjectBean> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        long chinese = 0;
        long english = 0;
        long math = 0;

        for (SubjectBean subjectBean : values) {
            if (subjectBean.getChinese() > 0) {
                chinese = subjectBean.getChinese();
            }

            if (subjectBean.getEnglish() > 0) {
                english = subjectBean.getEnglish();
            }

            if (subjectBean.getMath() > 0) {
                math = subjectBean.getMath();
            }
        }

        sum = chinese + english + math;
        SubjectBean subjectBean = new SubjectBean();
        subjectBean.setChinese(chinese);
        subjectBean.setEnglish(english);
        subjectBean.setMath(math);
        subjectBean.setTotal(sum);
        subjectBean.setAvg(sum / 3);

        context.write(key, new Text(subjectBean.toString()));
    }
}
