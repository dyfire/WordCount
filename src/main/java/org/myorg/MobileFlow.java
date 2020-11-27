package org.myorg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.myorg.model.FlowBean;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
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
public class MobileFlow {
    public static class Map extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            // 行内容转换为String
            String line = text.toString();

            // 按照tab切割字符
            StringTokenizer tokenizer = new StringTokenizer(line);

            // 拆分字符为数组
            ArrayList<String> rs = new ArrayList<>();
            while (tokenizer.hasMoreTokens()) {
                rs.add(tokenizer.nextToken().trim());
            }

            String mobile = rs.get(0);
            long upFlow = Long.parseLong(rs.get(1));
            long downFlow = Long.parseLong(rs.get(2));

            System.out.print(new FlowBean(upFlow, downFlow).toString());
            outputCollector.collect(new Text(mobile), new FlowBean(upFlow, downFlow));
        }
    }

    public static class Reduce extends MapReduceBase
            implements Reducer<Text, FlowBean, Text, IntWritable> {

        @Override
        public void reduce(Text text, Iterator<FlowBean> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            long sumUpFlow = 0;
            long sumDownFlow = 0;

            // 遍历所有,将上行和下行流量累加
            while (iterator.hasNext()) {
                sumUpFlow += iterator.next().getDownFlow();
                sumDownFlow += iterator.next().getUpFlow();
            }

            FlowBean flowBean = new FlowBean(sumUpFlow, sumDownFlow);
            outputCollector.collect(text, flowBean);
        }
    }

    public static void main(String[] args) throws Exception {
        File file = new File("output");
        if (file.exists() && file.isDirectory()) {
            file.delete();
        }

        JobConf conf = new JobConf(MobileFlow.class);

        // 指定job名称
        conf.setJobName("mobileFlow");

        // 指定jar中的启动类的class
        conf.setJarByClass(MobileFlow.class);

        // 指定map的class
        conf.setMapperClass(Map.class);

        // 指定reduce的class
        conf.setReducerClass(Reduce.class);

        // 指定map的输出数据格式
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(FlowBean.class);

        // 指定输出数据的输出格式
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(FlowBean.class);

        // 指定数据是输入格式
        conf.setInputFormat(TextInputFormat.class);

        // 指定数据的输出格式
        conf.setOutputFormat(TextOutputFormat.class);

        // 指定输出路径
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
