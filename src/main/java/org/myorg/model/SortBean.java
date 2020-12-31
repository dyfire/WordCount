package org.myorg.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortBean implements WritableComparable<SortBean> {
    private long first;
    private long second;

    public long getFirst() {
        return first;
    }

    public void setFirst(long first) {
        this.first = first;
    }

    public long getSecond() {
        return second;
    }

    public void setSecond(long second) {
        this.second = second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
         dataOutput.writeLong(this.first);
         dataOutput.writeLong(this.second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readLong();
        this.second = dataInput.readLong();
    }


    @Override
    public int compareTo(SortBean o) {
        if (o.getFirst() == this.first) {
            // second 降序
            return (int)(o.getSecond() - this.second);
        } else {
            // first 升序
            return (int)(this.first - o.getFirst());
        }
    }
}
