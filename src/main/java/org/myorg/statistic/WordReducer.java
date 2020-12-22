package org.myorg.statistic;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.myorg.model.WordBean;

import java.io.IOException;

public class WordReducer extends Reducer<Text, WordBean, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<WordBean> values, Context context)
            throws IOException, InterruptedException {
        long count = 0;
        WordBean word = new WordBean();
        word.setWord(key.toString());

        for (WordBean wordBean : values) {
            count++;
        }

        context.write(key, new Text(String.valueOf(count)));
    }
}
