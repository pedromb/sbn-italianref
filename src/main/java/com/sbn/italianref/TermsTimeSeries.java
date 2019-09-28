package com.sbn.italianref;

import java.time.LocalDateTime;
import java.util.Arrays;

public class TermsTimeSeries {

    int w;
    String term;
    double [] originalTs;
    LocalDateTime [] originalTsDates;
    double [] compressedTs;
    LocalDateTime[] compressedTsDates;
    String sax;

    public LocalDateTime[] getOriginalTsDates() {
        return originalTsDates;
    }

    public void setOriginalTsDates(LocalDateTime[] originalTsDates) {
        this.originalTsDates = originalTsDates;
    }

    public LocalDateTime[] getCompressedTsDates() {
        return compressedTsDates;
    }

    public void setCompressedTsDates(LocalDateTime[] compressedTsDates) {
        this.compressedTsDates = compressedTsDates;
    }


    public double[] getCompressedTs() {
        return compressedTs;
    }

    public void setCompressedTs(double[] compressedTs) {
        this.compressedTs = compressedTs;
    }

    public int getW() {
        return w;
    }

    public void setW(int w) {
        this.w = w;
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public double[] getOriginalTs() {
        return originalTs;
    }

    public void setOriginalTs(double[] originalTs) {
        this.originalTs = originalTs;
    }

    public void addToOriginalTs(double addValue, int position) {
        this.originalTs[position] += addValue;
    }

    public String getSax() {
        return sax;
    }

    public void setSax(String sax) {
        this.sax = sax;
    }

    public void printOriginalTs() {
        System.out.println("");
        System.out.print(this.term+": ");
        Arrays.stream(this.getOriginalTs()).forEach(x -> System.out.print(x+ " "));
        System.out.println("");

    }

    public void printCompresesdTs() {
        System.out.println("");
        System.out.print(this.term+": ");
        Arrays.stream(this.getCompressedTs()).forEach(x -> System.out.print(x+ " "));
        System.out.println("");

    }


    public void printSax() {
        System.out.println("");
        System.out.print(this.term+": "+this.getSax());
        System.out.println("");

    }

}
