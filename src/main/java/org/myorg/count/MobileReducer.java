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
        long upSum = 0;
        long downSum = 0;

        for (FlowBean flowBean : values) {
            FlowBean flow = new FlowBean();
            flow.setUpFlow(flowBean.upFlow);
            flow.setDownFlow(flowBean.downFlow);
            flow.setSumFlow(flowBean.sumFlow);

            upSum += flowBean.upFlow;
            downSum += flowBean.downFlow;
        }

        context.write(key, new Text(Long.toString(upSum) + "," + Long.toString(downSum)));
    }
}
