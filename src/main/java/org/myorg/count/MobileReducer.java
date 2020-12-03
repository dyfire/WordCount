package org.myorg.count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.myorg.model.FlowBean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MobileReducer extends Reducer<LongWritable, FlowBean, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {
        List<FlowBean> list = new ArrayList<FlowBean>();
        for (FlowBean flowBean : values) {
            FlowBean flow = new FlowBean();
            flow.setUpFlow(flowBean.upFlow);
            flow.setDownFlow(flowBean.downFlow);
            flow.setSumFlow(flowBean.sumFlow);

            System.out.println(flowBean.upFlow);
            list.add(flow);

            context.write(key, new Text(flow.toString()));
            System.out.println(flow.toString());
        }
    }
}
