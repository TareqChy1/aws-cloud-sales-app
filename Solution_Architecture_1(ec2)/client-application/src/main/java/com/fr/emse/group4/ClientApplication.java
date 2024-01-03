package com.fr.emse.group4;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Timestamp;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class ClientApplication {

    private static final String EC2_BASE_FILE_PATH = "/home/ec2-user/sales-data";
    private static final String LOCAL_BASE_FILE_PATH = "C:\\Users\\tareq\\OneDrive\\Desktop\\MSc_In_CPS2\\Third_Semester\\aws-cloud_course\\Project\\sales-data";

    public static void main(String[] args) {
        Region region = Region.US_EAST_1;
        String bucketName = "sales-data-input-bucket";
        String queueURL = "https://sqs.us-east-1.amazonaws.com/010405860157/data-processing-queue";

        if (isRunningOnEC2()) {
            System.out.println("Running on EC2 instance.");
        } else {
            System.out.println("Running on local machine.");
        }

        String baseFilePath = isRunningOnEC2() ? EC2_BASE_FILE_PATH : LOCAL_BASE_FILE_PATH;

        int[] storeIds = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}; // Add more store IDs as needed
        String[] dates = {"01-10-2022", "02-10-2022"}; // Add more dates as needed

        for (String date : dates) {
            for (int storeId : storeIds) {
                String fileName = date + "-store" + storeId + ".csv";
                String filePath = Paths.get(baseFilePath, fileName).toString();
                uploadAndNotify(region, bucketName, filePath, fileName, queueURL);
            }
        }
    }

    private static boolean isRunningOnEC2() {
        return new File(EC2_BASE_FILE_PATH).exists();
    }

    public static void uploadAndNotify(Region region, String bucketName, String filePath, String fileName, String queueURL) {
        if (uploadFile(region, bucketName, filePath, fileName)) {
            sendSQS(region, bucketName, fileName, queueURL);
        }
    }

    public static boolean uploadFile(Region region, String bucketName, String filePath, String fileName) {
        S3Client s3 = S3Client.builder().region(region).build();
        ListBucketsRequest listBucketRequest = ListBucketsRequest.builder().build();
        ListBucketsResponse listBucketResponse = s3.listBuckets(listBucketRequest);

        if ((listBucketResponse.hasBuckets()) && (listBucketResponse.buckets().stream().noneMatch(x -> x.name().equals(bucketName)))) {
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder().bucket(bucketName).build();
            s3.createBucket(bucketRequest);
        }

        File fileToUpload = new File(filePath);
        if (!fileToUpload.exists()) {
            System.out.println("File not found: " + filePath);
            return false;
        }

        if (!isFileInBucket(s3, bucketName, fileName)) {
            PutObjectRequest putOb = PutObjectRequest.builder().bucket(bucketName).key(fileName).build();
            s3.putObject(putOb, RequestBody.fromBytes(getObjectFile(filePath)));
            System.out.println("The file " + fileName + " is uploaded to bucket " + bucketName);
            return true;
        } else {
            System.out.println("The file " + fileName + " already exists in bucket " + bucketName + ". Skipping upload.");
            return false;
        }
    }

    private static boolean isFileInBucket(S3Client s3, String bucketName, String fileName) {
        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileName)
                    .build();
            HeadObjectResponse headObjectResponse = s3.headObject(headObjectRequest);
            return true; // The file exists
        } catch (NoSuchKeyException e) {
            return false; // The file does not exist
        }
    }

    private static byte[] getObjectFile(String filePath) {
        byte[] bytesArray = null;

        try (FileInputStream fileInputStream = new FileInputStream(new File(filePath))) {
            bytesArray = new byte[(int) new File(filePath).length()];
            fileInputStream.read(bytesArray);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytesArray;
    }

    public static void sendSQS(Region region, String bucketName, String fileName, String queueURL) {
        SqsClient sqsClient = SqsClient.builder().region(region).build();
        SendMessageRequest sendRequest = SendMessageRequest.builder()
                .queueUrl(queueURL)
                .messageBody(bucketName + ";" + fileName)
                .build();

        SendMessageResponse sqsResponse = sqsClient.sendMessage(sendRequest);
        System.out.println(sqsResponse.messageId() + " Message sent. Status is " + sqsResponse.sdkHttpResponse().statusCode());
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println("Notified the Worker Java application at : " + "[" + timestamp + "]");
    }
}
