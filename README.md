# AWS Cloud Sales Application

__A global retail sales data summarization and consolidation service using AWS services__
- - - -

## Context
<p align="justify">
In the proposed AWS Cloud Sales Application project for a global retailer, we have two solution architectures for managing daily sales data. Both solutions involve three applications: Client, Worker, and Consolidator. The first solution has the Client Application uploading daily sales data in CSV format to an Amazon S3 input bucket, with Amazon SQS notifying the Worker Application. This Worker Application processes the CSV files, summarizing sales data by store(Total profit) and by product(Total quantity, Total sold, Total profit), and saves it to an S3 output bucket, ensuring robust processing under varying loads. The Consolidator Application, run manually, processes data from a specific date, aggregating key metrics like total retailer’s profit, the most and least profitable stores, and the total quantity, total sold, and total profit per product. The second solution is similar, but Client Application uses Amazon SNS for notifications and AWS Lambda for the Worker Application, enhancing scalability and fault tolerance. The Consolidator Application's role remains unchanged, providing crucial sales insights.

## Project Folder Structure
<p align="center">
  <img src="Directory_image.png">
  <br>
  <em></em>
</p>

## Required Tools and Services
Pre-requisite tools for the environment setup.
- Java 17
- Apache Maven 3.9.5+
- AWS SDK for Java 2.x

The solution architecture used the following AWS services:
- Amazon EC2
- AWS Lambda
- Amazon S3
- Amazon SQS
- Amazon SNS

## Development Steps:

### Solution 1: Traditional Server-Based Approach (Worker Application (Java on EC2))

- Create an EC2 instance on AWS cloud for hosting the Worker Java application, we followed the provided steps in this [link](https://ci.mines-stetienne.fr/cps2/course/cloud/labs/01-aws-ec2.html).

- Create two Amazon S3 buckets: one, introduced as "sales-data-input-bucket," for the Client Application to upload daily sales data, and another, known as "sales-data-output-bucket," for the Worker Application to store processed and summarized data, by following the steps provided in this [link](https://ci.mines-stetienne.fr/cps2/course/cloud/labs/02-aws-s3.html).

- Create an Amazon SQS queue to facilitate communication between the Client Application and the Worker Application on EC2 by following the steps in this [link](https://ci.mines-stetienne.fr/cps2/course/cloud/labs/05-aws-sqs-sns.html).

- Build the Maven .jar file for the worker java application using IDEs such as VSCode or IntelliJ IDEA, ensuring the file is named with the suffix “*-jar-with-dependencies.jar” (e.g., worker-java-application-1.0-SNAPSHOT-jar-with-dependencies.jar).

- Connect to the EC2 instance using PuTTY by following the provied steps in this [link](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/putty.html#putty-ssh). 

- Upload the built .jar file, worker-java-application-1.0-SNAPSHOT-jar-with-dependencies.jar, to the EC2 instance using SFTP client like fileZilla by following the provied steps in this [link](https://ci.mines-stetienne.fr/cps2/course/cloud/labs/01-aws-ec2.html).

- Run the worker java application on EC2 instance using PuTTY. 


### Solution 2: Serverless Approach (Worker Application (AWS Lambda Function))

- Create two Amazon S3 buckets: already created in solution one. 
- Create Amazon SNS for the Client Application to notify the worker Lambda function, as described in this [link](https://ci.mines-stetienne.fr/cps2/course/cloud/labs/05-aws-sqs-sns.html).
- Create a Lambda function on AWS Console and add the created SNS trigger using the steps provided in this [link](https://ci.mines-stetienne.fr/cps2/course/cloud/labs/04-aws-lambda.html).

- Build the Maven .jar file for the Worker lambda function using IDEs such as VSCode or IntelliJ IDEA, ensuring the file is named  worker-lambda-application-1.0-SNAPSHOT.jar. 

- Upload this .jar(worker-lambda-application-1.0-SNAPSHOT.jar) file in the AWS lambda function using the steps provided in this [link](https://ci.mines-stetienne.fr/cps2/course/cloud/labs/04-aws-lambda.html).


## Run The Project

### Solution 1: Traditional Server-Based Approach (Worker Application (Java on EC2))

- Run the Client Application on the local machine using IntelliJ or VSCode. The application will upload sales data to the AWS S3 input bucket *sales-data-input-bucket* and notify a worker application via SQS.
- Check the files uploaded in AWS S3 input bucket named *sales-data-input-bucket*.
- Run the Worker Application on the EC2 instance using the command: `java -jar worker-java-application-1.0-SNAPSHOT-jar-with-dependencies.jar`. The application connects to AWS S3 and SQS, monitors the SQS queue for new messages, processes files from *sales-data-input-bucket* and writes results to a new CSV file. Upload the processed data to the *sales-data-output-bucket* and clean up the original files from the S3 input bucket *sales-data-input-bucket* and SQS messages.
- Check the files uploaded in AWS S3 output bucket named *sales-data-output-bucket*.
- Run the Consolidator Application on the local machine using IntelliJ or VSCode. The application will retrieve and process data from the AWS S3 bucket *sales-data-output-bucket* based on the specified date input.

### Solution 2: Serverless Approach (Worker Application (AWS Lambda Function))

- Run the Client Application on the local machine using IntelliJ or VSCode.This application automatically uploads sales data files to the AWS S3 bucket *sales-data-input-bucket* and notifies worker Lambda function via Amazon SNS.
- Check the files uploaded in AWS S3 input bucket named *sales-data-input-bucket*.
- Check the monitor logs on AWS Lambda function console. When AWS Lambda function triggered by SNS notifications. The WorkerLambda function processes the sales data: it reads S3 bucket names and file names from SNS messages, downloads files from S3, processes the sales data, and calculates profits and product summaries. Write the results to a CSV file and upload it to the *sales-data-output-bucket* on S3. Post-processing, it removes the original file from AWS S3 bucket *sales-data-input-bucket*.
- Check the files uploaded in AWS S3 output bucket named *sales-data-output-bucket*.
- Run the Consolidator Application on the local machine using IntelliJ or VSCode. The application will retrieve and process data from the AWS S3 bucket *sales-data-output-bucket* based on the specified date input.