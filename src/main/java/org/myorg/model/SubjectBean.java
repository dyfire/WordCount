package org.myorg.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SubjectBean implements Writable {
    private long chinese;
    private long english;
    private long math;
    private long total;
    private long avg;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.chinese);
        dataOutput.writeLong(this.english);
        dataOutput.writeLong(this.math);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.chinese = dataInput.readLong();
        this.english = dataInput.readLong();
        this.math = dataInput.readLong();
    }

    public long getChinese() {
        return chinese;
    }

    public void setChinese(long chinese) {
        this.chinese = chinese;
    }

    public long getEnglish() {
        return english;
    }

    public void setEnglish(long english) {
        this.english = english;
    }

    public long getMath() {
        return math;
    }

    public void setMath(long math) {
        this.math = math;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long getAvg() {
        return avg;
    }

    public void setAvg(long avg) {
        this.avg = avg;
    }

    @Override
    public String toString() {
        return  chinese +
                "," + english +
                "," + math +
                "," + total +
                "," + avg;
    }
}
