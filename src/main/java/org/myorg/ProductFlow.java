package org.myorg;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.myorg.model.GoodBean;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class ProductFlow {
    public static class Map extends Mapper<LongWritable, Text, Text, GoodBean> {
        GoodBean bean = new GoodBean();
        Text text = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 获取输入文件名称
            FileSplit split = (FileSplit) context.getInputSplit();
            String name = split.getPath().getName();

            String[] line = value.toString().split(",");

            // 根据不同的文件(理解为不同的表)做业务逻辑处理
            if (name.startsWith("order")) {
                // t_order(id,product_id,amount)
                bean.setOrderId(line[0]);
                bean.setProductId(line[1]);
                bean.setAmount(Integer.parseInt(line[2]));
                bean.setProductName("");
                bean.setFlag("0");
                text.set(line[1]);
            } else {
                // t_product(product_id,name)
                bean.setProductId(line[0]);
                bean.setProductName(line[1]);
                bean.setOrderId("");
                bean.setAmount(0);
                bean.setFlag("1");
                text.set(line[0]);
            }

            context.write(text, bean);
        }
    }

    public static class Reduce extends Reducer<Text, GoodBean, GoodBean, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<GoodBean> values, Context context)
                throws IOException, InterruptedException {
            // 存储订单集合
            ArrayList<GoodBean> goodBeans = new ArrayList<>();

            // 产品对象
            GoodBean productBean = new GoodBean();

            // 0:订单,1:产品
            for (GoodBean goodBean : values) {
                if ("0".equals(goodBean.getFlag())) {
                    GoodBean orderBean = new GoodBean();
                    try {
                        BeanUtils.copyProperties(orderBean, goodBean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    goodBeans.add(orderBean);
                } else {
                    try {
                        BeanUtils.copyProperties(productBean, goodBean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            // 表的拼接(join)
            for (GoodBean bean : goodBeans) {
                bean.setProductName(productBean.getProductName());

                context.write(bean, NullWritable.get());
            }
        }
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        Tool.deleteDir(new File("output"));

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(ProductFlow.class);

        job.setMapperClass(ProductFlow.Map.class);
        job.setReducerClass(ProductFlow.Reduce.class);

//        job.setSortComparatorClass(ProductFlow.Comparator.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(GoodBean.class);

        job.setOutputKeyClass(GoodBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean ret = job.waitForCompletion(true);
        System.exit(ret ? 1 : 0);
    }
}
