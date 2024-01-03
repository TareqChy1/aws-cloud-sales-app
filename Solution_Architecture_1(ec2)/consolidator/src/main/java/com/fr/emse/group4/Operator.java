package com.fr.emse.group4;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Operator {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter the date in your preferred format (e.g., DD-MM-YYYY): ");
        String date = scanner.nextLine();

        String bucketName = "sales-data-output-bucket";
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        System.out.println("Getting files from the bucket for date: " + date);

        List<S3ObjectSummary> filteredObjects = new ArrayList<>();
        for (S3ObjectSummary object : s3Client.listObjects(bucketName).getObjectSummaries()) {
            if (object.getKey().contains("Summary-" + date)) {
                filteredObjects.add(object);
                System.out.println("File added for processing: " + object.getKey());
            }
        }

        // Add a print message here
        if (!filteredObjects.isEmpty()) {
            System.out.println("Files retrieved successfully. Sending files to the Consolidator for processing.");
            Consolidator consolidator = new Consolidator();
            consolidator.processData(filteredObjects, bucketName);
        } else {
            System.out.println("No files found for the given date: " + date);
        }

        scanner.close();
    }
}
