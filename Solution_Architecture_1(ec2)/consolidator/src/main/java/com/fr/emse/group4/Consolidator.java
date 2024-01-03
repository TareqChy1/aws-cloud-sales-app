package com.fr.emse.group4;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

public class Consolidator {

    public void processData(List<S3ObjectSummary> objects, String bucketName) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        Map<String, Double> profitOfEachStoreList = new HashMap<>();
        Map<String, Double> quantitySoldProductList = new HashMap<>();
        Map<String, Double> profitEachProductList = new HashMap<>();
        Map<String, Double> soldEachProductList = new HashMap<>();
        Double totalRetailersProfit = 0.0;

        for (S3ObjectSummary obj : objects) {
            try (S3Object s3Object = s3Client.getObject(bucketName, obj.getKey());
                 InputStreamReader streamReader = new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8);
                 BufferedReader reader = new BufferedReader(streamReader)) {

                String line;
                boolean isFirstLine = true;

                while ((line = reader.readLine()) != null) {
                    if (isFirstLine) {
                        isFirstLine = false;
                        continue;
                    }

                    String[] rows = line.replace("\"", "").split(",");
                    if (rows.length < 5) {
                        System.err.println("Unexpected number of columns in row: " + line);
                        continue;
                    }

                    String type = rows[0].trim().toLowerCase();
                    String name = rows[1].trim();
                    Double quantity = type.equals("product") ? parseDouble(rows[2]) : 0.0;
                    Double sold = type.equals("product") ? parseDouble(rows[3]) : 0.0;
                    Double profit = parseDouble(rows[4]);

                    if ("product".equals(type)) {
                        quantitySoldProductList.merge(name, quantity, Double::sum);
                        profitEachProductList.merge(name, profit, Double::sum);
                        soldEachProductList.merge(name, sold, Double::sum);
                    } else if ("store".equals(type)) {
                        profitOfEachStoreList.merge(name, profit, Double::sum);
                        totalRetailersProfit += profit;
                    } else {
                        System.err.println("Unknown type in row: " + line);
                    }
                }

            } catch (IOException e) {
                System.err.println("Error processing file: " + obj.getKey() + " - " + e.getMessage());
            }
        }

        displayResults(profitOfEachStoreList, quantitySoldProductList, profitEachProductList, soldEachProductList, totalRetailersProfit);
    }

    private Double parseDouble(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 0.0;
        }

        try {
            // Convert scientific notation to the correct numeric format
            DecimalFormat decimalFormat = new DecimalFormat("0.00");
            String formattedValue = decimalFormat.format(Double.parseDouble(value));
            return Double.parseDouble(formattedValue);
        } catch (NumberFormatException e) {
            System.err.println("Invalid number format: " + value);
            return 0.0;
        }
    }

    private void displayResults(Map<String, Double> profitOfEachStoreList, Map<String, Double> quantitySoldProductList,
                                Map<String, Double> profitEachProductList, Map<String, Double> soldEachProductList,
                                Double totalRetailersProfit) {
        System.out.println("Total retailer's profit: " + String.format("%.2f", totalRetailersProfit));

        if (!profitOfEachStoreList.isEmpty()) {
            Entry<String, Double> mostProfitableStore = Collections.max(profitOfEachStoreList.entrySet(), Entry.comparingByValue());
            Entry<String, Double> leastProfitableStore = Collections.min(profitOfEachStoreList.entrySet(), Entry.comparingByValue());

            System.out.println("Most profitable store: " + mostProfitableStore.getKey() + " - Profit: " + String.format("%.2f", mostProfitableStore.getValue()));
            System.out.println("Least profitable store: " + leastProfitableStore.getKey() + " - Profit: " + String.format("%.2f", leastProfitableStore.getValue()));
        } else {
            System.out.println("No data available for profit calculation.");
        }

        System.out.println("Profit of each store:");
        profitOfEachStoreList.forEach((store, profit) -> System.out.println(store + ": " + String.format("%.2f", profit)));

        System.out.println("Total quantity sold per product:");
        quantitySoldProductList.forEach((product, quantity) -> System.out.println(product + ": " + String.format("%.0f", quantity)));

        System.out.println("Total profit per product:");
        profitEachProductList.forEach((product, profit) -> System.out.println(product + ": " + String.format("%.2f", profit)));

        System.out.println("Total sold per product:");
        soldEachProductList.forEach((product, sold) -> System.out.println(product + ": " + String.format("%.2f", sold)));
    }
}
