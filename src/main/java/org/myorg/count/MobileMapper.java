package org.myorg.count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.myorg.model.FlowBean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * 计算统计用户手机流量
 * 18616123067,200,1100
 * 需要同一个用户的上行流量,下行流量累加汇总
 * <p>
 * 18616123067,500,1600,2200
 * input <行号,该行数据>
 * output <手机号码,<上行流量、下行流量、总流量>>
 * 比如:
 * <13897230503,<upFlow:400, dFlow:1300, sumFlow:1700>>
 */
public class MobileMapper extends Mapper<LongWritable, Text, LongWritable, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 行内容转换为String
        String line = value.toString();

        // 按照tab切割字符
        StringTokenizer tokenizer = new StringTokenizer(line, ",");

        // 拆分字符为数组
        ArrayList<String> rs = new ArrayList<>();
        while (tokenizer.hasMoreTokens()) {
            rs.add(tokenizer.nextToken().trim());
        }

        long mobile = Long.parseLong(rs.get(0));
        long upFlow = Long.parseLong(rs.get(1));
        long downFlow = Long.parseLong(rs.get(2));

        FlowBean flowBean = new FlowBean(upFlow, downFlow);

        context.write(new LongWritable(mobile), flowBean);
    }
}
