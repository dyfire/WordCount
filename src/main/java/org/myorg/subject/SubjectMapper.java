package org.myorg.subject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.myorg.model.SubjectBean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class SubjectMapper extends Mapper<LongWritable, Text, Text, SubjectBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        ArrayList<String> rs = new ArrayList<>();

        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), "|");
        while (stringTokenizer.hasMoreTokens()) {
            rs.add(stringTokenizer.nextToken().trim());
        }

        long chinese = 0;
        long english = 0;
        long math = 0;

        switch (rs.get(0)) {
            case "a":
                chinese = Long.parseLong(rs.get(2));
                break;
            case "b":
                english = Long.parseLong(rs.get(2));
                break;
            case "c":
                math = Long.parseLong(rs.get(2));
                break;
        }

        String name = rs.get(1);
        SubjectBean bean = new SubjectBean();
        bean.setChinese(chinese);
        bean.setEnglish(english);
        bean.setMath(math);

        context.write(new Text(name), bean);
    }
}
