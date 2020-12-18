package org.myorg.model;

public class WordBean {
    private long number;
    private String word;
    private String filename;

    public WordBean() {

    }

    public WordBean(String word, String filename) {
        this.word = word;
        this.filename = filename;
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
}
