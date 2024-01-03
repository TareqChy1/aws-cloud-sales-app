package com.fr.emse.group4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.opencsv.CSVWriter;

public class WorkerLambda implements RequestHandler<SNSEvent, String> {

    private static AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

    @Override
    public String handleRequest(SNSEvent event, Context context) {
        context.getLogger().log("Invocation started.");

        if (event.getRecords().isEmpty()) {
            context.getLogger().log("No records found in the event.");
            return "No Records";
        }

        String message = event.getRecords().get(0).getSNS().getMessage();
        context.getLogger().log("Received message: " + message);

        String[] arguments = message.split(";");
        if (arguments.length < 2) {
            context.getLogger().log("Invalid message format.");
            return "Invalid Format";
        }

        String inputBucketName = arguments[0];
        String fileName = arguments[1];

        context.getLogger().log("Received message to process file: " + fileName);
        try (S3Object s3Object = s3.getObject(inputBucketName, fileName);
             InputStreamReader streamReader = new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(streamReader)) {

            context.getLogger().log("Downloading and processing file: " + fileName);
            processFile(reader, fileName, context);
            context.getLogger().log("File processing complete. Deleting file from S3 bucket: " + inputBucketName);
            deleteFileFromS3(inputBucketName, fileName, context);
        } catch (IOException e) {
            context.getLogger().log("Error occurred: " + e.getMessage());
            return "Error";
        }

        context.getLogger().log("Invocation completed.");
        return "OK";
    }

    private static void processFile(BufferedReader reader, String fileName, Context context) throws IOException {
        Map<String, Double> storeTotalProfit = new HashMap<>();
        Map<String, ProductSummary> productSummaryMap = new HashMap<>();

        String line;
        reader.readLine(); // Skip header
        context.getLogger().log("Reading data from file.");
        while ((line = reader.readLine()) != null) {
            String[] rows = line.split(";");
            if (rows.length < 7) {
                context.getLogger().log("Skipping malformed line: " + line);
                continue;
            }
            String storeName = rows[1];
            String product = rows[2];
            int quantity = Integer.parseInt(rows[3]);
            double unitPrice = Double.parseDouble(rows[4]);
            double unitProfit = Double.parseDouble(rows[6]);

            storeTotalProfit.merge(storeName, unitProfit * quantity, Double::sum);
            productSummaryMap.computeIfAbsent(product, k -> new ProductSummary())
                    .addValues(quantity, unitPrice * quantity, unitProfit * quantity);
        }

        context.getLogger().log("Writing data to summary CSV.");
        writeToCSV(storeTotalProfit, productSummaryMap, fileName, context);
    }

    static class ProductSummary {
        int totalQuantity;
        double totalSold;
        double totalProfit;

        void addValues(int quantity, double sold, double profit) {
            this.totalQuantity += quantity;
            this.totalSold += sold;
            this.totalProfit += profit;
        }
    }

    private static void writeToCSV(Map<String, Double> storeTotalProfit, Map<String, ProductSummary> productSummaryMap, String fileName, Context context) throws IOException {
        String tempDir = System.getProperty("java.io.tmpdir");
        String outputFilename = tempDir + File.separator + "Summary-" + fileName;

        try (CSVWriter csvWriter = new CSVWriter(new FileWriter(outputFilename))) {
            context.getLogger().log("Creating CSV file: " + outputFilename);
            String[] header = {"Type", "Name", "Total Quantity", "Total Sold", "Total Profit"};
            csvWriter.writeNext(header);

            storeTotalProfit.forEach((store, totalProfit) -> csvWriter.writeNext(new String[]{"Store", store, "", "", String.valueOf(totalProfit)}));
            productSummaryMap.forEach((product, summary) -> csvWriter.writeNext(new String[]{"Product", product, String.valueOf(summary.totalQuantity), String.valueOf(summary.totalSold), String.valueOf(summary.totalProfit)}));
        }

        File file = new File(outputFilename);
        s3.putObject("sales-data-output-bucket", "Summary-" + fileName, file); // Replace with your output S3 bucket name
        context.getLogger().log("Summary file uploaded to sales-data-output-bucket S3 bucket");
    }

    private static void deleteFileFromS3(String bucketName, String fileName, Context context) {
        try {
            s3.deleteObject(bucketName, fileName);
            context.getLogger().log("Deleted file " + fileName + " from S3 bucket: " + bucketName);
        } catch (Exception e) {
            context.getLogger().log("Error occurred while trying to delete file " + fileName + " from S3 bucket: " + e.getMessage());
        }
    }
}
