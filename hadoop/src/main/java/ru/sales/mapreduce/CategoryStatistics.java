package ru.sales.mapreduce;

public class CategoryStatistics {
    String category;
    double revenue;
    int quantity;

    public CategoryStatistics(String category, double revenue, int quantity) {
        this.category = category;
        this.revenue = revenue;
        this.quantity = quantity;
    }
}