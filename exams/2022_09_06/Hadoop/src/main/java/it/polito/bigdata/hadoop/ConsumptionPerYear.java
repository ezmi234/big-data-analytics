package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ConsumptionPerYear implements org.apache.hadoop.io.Writable {
    private int year2020 = 0;
    private int year2021 = 0;
    
    public int getYear2020(){
        return year2020;
    }

    public int getYear2021(){
        return year2021;
    }

    public void setYear2020(int year){
        this.year2020 = year;
    }

    public void setYear2021(int year){
        this.year2021 = year;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year2020);
        out.writeInt(year2021);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year2020 = in.readInt();
        year2021 = in.readInt();
    }

    
}