package com.fr.emse.group4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.opencsv.CSVWriter;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class WorkerJavaApplication {

    private static AmazonS3 s3;

    public static void main(String[] args) {
        Timestamp startTimestamp = new Timestamp(System.currentTimeMillis());
        System.out.println("Invocation started : " + "[" + startTimestamp + "]");

        Region region = Region.US_EAST_1;
        String queueURL = "https://sqs.us-east-1.amazonaws.com/010405860157/data-processing-queue";
        SqsClient sqsClient = SqsClient.builder().region(region).build();
        s3 = AmazonS3ClientBuilder.defaultClient();

        boolean continueProcessing = true;
        while (continueProcessing) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueURL)
                    .maxNumberOfMessages(1)
                    .build();

            List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

            if (!messages.isEmpty()) {
                System.out.println("Processing message from SQS queue.");
                processMessage(messages.get(0), sqsClient, queueURL);
            } else {
                System.out.println("No messages in the queue.");
                continueProcessing = false;  // Exit loop when no messages are left
            }
        }

        Timestamp endTimestamp = new Timestamp(System.currentTimeMillis());
        System.out.println("Invocation completed : " + "[" + endTimestamp + "]");
    }

    private static void processMessage(Message message, SqsClient sqsClient, String queueURL) {
        String[] arguments = message.body().split(";");
        String inputBucketName = arguments[0];
        String fileName = arguments[1];

        System.out.println("Received message to process file: " + fileName);
        try (S3Object s3Object = s3.getObject(inputBucketName, fileName);
             InputStreamReader streamReader = new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(streamReader)) {

            System.out.println("Downloading and processing file: " + fileName);
            processFile(reader, fileName);
            System.out.println("File processing complete, deleting file from S3 bucket: " + inputBucketName);
            deleteFileFromS3(inputBucketName, fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Deleting processed message from SQS queue.");
        deleteMessageFromQueue(sqsClient, queueURL, message);
    }

    private static void processFile(BufferedReader reader, String fileName) throws IOException {
        Map<String, Double> storeTotalProfit = new HashMap<>();
        Map<String, ProductSummary> productSummaryMap = new HashMap<>();

        reader.readLine(); // skip header
        System.out.println("Reading data from file.");
        reader.lines().forEach(line -> {
            String[] rows = line.split(";");
            String storeName = rows[1];
            String product = rows[2];
            int quantity = Integer.parseInt(rows[3]);
            double unitPrice = Double.parseDouble(rows[4]);
            double unitProfit = Double.parseDouble(rows[6]);

            storeTotalProfit.merge(storeName, unitProfit * quantity, Double::sum);

            productSummaryMap.computeIfAbsent(product, k -> new ProductSummary())
                             .addValues(quantity, unitPrice * quantity, unitProfit * quantity);
        });

        System.out.println("Writing data to summary CSV.");
        writeToCSV(storeTotalProfit, productSummaryMap, fileName);
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

    private static void writeToCSV(Map<String, Double> storeTotalProfit, Map<String, ProductSummary> productSummaryMap, String fileName) throws IOException {
        String tempDir = System.getProperty("java.io.tmpdir");
        String outputFilename = tempDir + File.separator + "Summary-" + fileName;

        File tempDirectory = new File(tempDir);
        if (!tempDirectory.exists()) {
            System.out.println("Temporary directory not found, creating directory.");
            tempDirectory.mkdir();
        }

        try (CSVWriter csvWriter = new CSVWriter(new FileWriter(outputFilename))) {
            System.out.println("Creating CSV file: " + outputFilename);
            String[] header = {"Type", "Name", "Total Quantity", "Total Sold", "Total Profit"};
            csvWriter.writeNext(header);

            storeTotalProfit.forEach((store, totalProfit) -> {
                String[] row = {"Store", store, "", "", String.valueOf(totalProfit)};
                csvWriter.writeNext(row);
            });

            productSummaryMap.forEach((product, summary) -> {
                String[] row = {"Product", product, String.valueOf(summary.totalQuantity), String.valueOf(summary.totalSold), String.valueOf(summary.totalProfit)};
                csvWriter.writeNext(row);
            });
        }

        File file = new File(outputFilename);
        s3.putObject("sales-data-output-bucket", "Summary-" + fileName, file);
        System.out.println("Summary file uploaded to sales-data-output-bucket");
    }

    private static void deleteFileFromS3(String bucketName, String fileName) {
        try {
            s3.deleteObject(bucketName, fileName);
            System.out.println("Deleted file " + fileName + " from S3 bucket: " + bucketName);
        } catch (Exception e) {
            System.err.println("Error occurred while trying to delete file " + fileName + " from S3 bucket: " + e.getMessage());
        }
    }

    private static void deleteMessageFromQueue(SqsClient sqsClient, String queueURL, Message message) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueURL)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
        System.out.println("Message deleted from SQS queue.");
    }
}
