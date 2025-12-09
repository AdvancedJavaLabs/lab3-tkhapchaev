package ru.sales.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class SalesReducer extends Reducer<Text, SalesWritable, Text, Text> {
    private final List<CategoryStatistics> statistics = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<SalesWritable> values, Context context) {
        double totalRevenue = 0.0;
        int totalQuantity = 0;

        for (SalesWritable salesWritable : values) {
            totalRevenue += salesWritable.getRevenue();
            totalQuantity += salesWritable.getQuantity();
        }

        statistics.add(new CategoryStatistics(key.toString(), totalRevenue, totalQuantity));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<CategoryStatistics> statisticsSorted = statistics
                .parallelStream()
                .sorted(Comparator.comparingDouble((CategoryStatistics categoryStatistics) -> categoryStatistics.revenue).reversed())
                .collect(Collectors.toList());

        Text outKey = new Text();
        Text outValue = new Text();

        outValue.set(String.format("%-20s %15s %10s", "Category", "Revenue", "Quantity"));
        context.write(outKey, outValue);

        outValue.set(String.format("%-20s %15s %10s", "--------------------", "---------------", "----------"));
        context.write(outKey, outValue);

        for (CategoryStatistics categoryStatistics : statisticsSorted) {
            String line = String.format(
                    Locale.US,
                    "%-20s %15.2f %10d",
                    categoryStatistics.category,
                    categoryStatistics.revenue,
                    categoryStatistics.quantity
            );

            outValue.set(line);
            context.write(outKey, outValue);
        }
    }
}