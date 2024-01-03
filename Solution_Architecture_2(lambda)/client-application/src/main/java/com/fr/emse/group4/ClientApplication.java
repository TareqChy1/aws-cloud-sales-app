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
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

public class ClientApplication {

    private static final String LOCAL_BASE_FILE_PATH = "C:\\Users\\tareq\\OneDrive\\Desktop\\MSc_In_CPS2\\Third_Semester\\aws-cloud_course\\Project\\sales-data"; // Local execution path
    private static final String EC2_BASE_FILE_PATH = "/home/ec2-user/sales-data"; // EC2 instance path

    public static void main(String[] args) {
        Region region = Region.US_EAST_1;
        String bucketName = "sales-data-input-bucket";
        String topicArn = "arn:aws:sns:us-east-1:010405860157:sales-data-process-sns";

        // Determine the base file path based on the execution environment
        String baseFilePath;
        String execEnv = System.getenv("AWS_EXECUTION_ENV");

        if (execEnv != null && execEnv.contains("EC2")) {
            baseFilePath = EC2_BASE_FILE_PATH;
            System.out.println("Running on EC2 Instance.");
        } else if (execEnv != null) {
            baseFilePath = "/tmp"; // Lambda temporary directory
            System.out.println("Running on AWS Lambda.");
        } else {
            baseFilePath = LOCAL_BASE_FILE_PATH;
            System.out.println("Running Locally.");
        }

        int[] storeIds = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}; // Add more store IDs as needed
        String[] dates = {"01-10-2022", "02-10-2022"}; // Add more dates as needed

        for (String date : dates) {
            for (int storeId : storeIds) {
                String fileName = date + "-store" + storeId + ".csv";
                String filePath = Paths.get(baseFilePath, fileName).toString();
                uploadAndNotify(region, bucketName, filePath, fileName, topicArn);
            }
        }
    }

    public static void uploadAndNotify(Region region, String bucketName, String filePath, String fileName, String topicArn) {
        if (uploadFile(region, bucketName, filePath, fileName)) {
            publishToSNS(region, bucketName, fileName, topicArn);
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

    public static void publishToSNS(Region region, String bucketName, String fileName, String topicArn) {
        SnsClient snsClient = SnsClient.builder().region(region).build();
        String message = bucketName + ";" + fileName;

        PublishRequest publishRequest = PublishRequest.builder()
                .topicArn(topicArn)
                .message(message)
                .build();

        PublishResponse publishResponse = snsClient.publish(publishRequest);
        System.out.println(publishResponse.messageId() + " Message published. Status is " + publishResponse.sdkHttpResponse().statusCode());
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println("Notified the Worker Lambda at : " + "[" + timestamp + "]");
    }
}
