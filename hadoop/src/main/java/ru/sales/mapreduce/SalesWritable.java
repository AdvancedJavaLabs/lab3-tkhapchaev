package ru.sales.mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SalesWritable implements Writable {
    private double revenue;
    private int quantity;

    public void set(double revenue, int quantity) {
        this.revenue = revenue;
        this.quantity = quantity;
    }

    public double getRevenue() {
        return revenue;
    }

    public int getQuantity() {
        return quantity;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(revenue);
        dataOutput.writeInt(quantity);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        revenue = dataInput.readDouble();
        quantity = dataInput.readInt();
    }
}