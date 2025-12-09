package ru.sales.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalesMapper extends Mapper<LongWritable, Text, Text, SalesWritable> {
    private static final int CATEGORY_INDEX = 2;
    private static final int PRICE_INDEX = 3;
    private static final int QUANTITY_INDEX = 4;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();

        if (line.isEmpty()) {
            return;
        }

        if (key.get() == 0 && line.startsWith("transaction_id")) {
            return;
        }

        String[] parts = line.split(",");

        if (parts.length < 5) {
            return;
        }

        String category = parts[CATEGORY_INDEX];
        double price = Double.parseDouble(parts[PRICE_INDEX]);
        int quantity = Integer.parseInt(parts[QUANTITY_INDEX]);

        double revenue = price * quantity;

        Text outKey = new Text(category);
        SalesWritable outValue = new SalesWritable();

        outKey.set(category);
        outValue.set(revenue, quantity);

        context.write(outKey, outValue);
    }
}