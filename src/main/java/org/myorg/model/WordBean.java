package org.myorg.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordBean implements Writable {
    private long number;
    private String word;
    private String filename;

    public WordBean() {

    }

    public WordBean(String word, String filename, long number) {
        this.word = word;
        this.filename = filename;
        this.number = number;
    }

    public long getNumber() {
        return number;
    }

    public void setNumber(long number) {
        this.number = number;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    @Override
    public String toString() {
        return word + "," + number + "," + filename;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeChars(this.word);
        dataOutput.writeChars(this.filename);
        dataOutput.writeLong(this.number);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word = dataInput.readLine();
        this.filename = dataInput.readLine();
//        this.number = dataInput.readLong();
    }
}
