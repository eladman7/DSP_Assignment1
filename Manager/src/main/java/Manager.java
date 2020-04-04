import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Manager {

    public static void main(String[] args) {
        // Currently assuming there is only one LocalApplication.

        Region region = Region.US_EAST_1;
        List<Message> messages;
        Message inputMessage;
//        String sqsName = args[0];
        final String sqsName = "Local_Manager_Queue";           // Save the name of the Local <--> Manager sqs
//        int numOfMsgForWorker = Integer.parseInt(args[1]);
        int numOfMsgForWorker = 1;                          // Save number of msg for each worker
        SqsClient sqsClient = SqsClient.builder().region(region).build(); // Build Sqs client
        S3Client s3 = S3Client.builder().region(region).build();             // Build S3 client
        Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();


        String localManagerUrl = getQUrl(sqsName, sqsClient);
        ReceiveMessageRequest rRLocalManager;

        ExecutorService executor = Executors.newCachedThreadPool();
        ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;


        // Connect to the Queue

        while (true) {
            try {
                rRLocalManager = ReceiveMessageRequest.builder()
                        .queueUrl(localManagerUrl)   //which queue
                        .build();
                break;
            } catch (Exception ignored) {
            }
        }


        // Read message from Local
        // Busy Wait until terminate message

      while (true) {
        try {
            messages = sqsClient.receiveMessage(rRLocalManager).messages();
            inputMessage = messages.get(0);
            if (inputMessage.body().equals("terminate") && messages.size() == 1) {
                System.out.println("manager get terminate message, deleting terminate message");

                deleteMessageFromQ(inputMessage, sqsClient, localManagerUrl);
                System.out.println("waiting for all local apps connections to finish");

                executor.shutdown();
                waitExecutorToFinish(executor);
                System.out.println("terminating ec2 instances.. ");

                terminateEc2Instances(ec2);
                System.out.println("succeed terminate all ec2 instances, quiting.. Bye");

                // TODO: 04/04/2020 Check if makeAndUpload needed!
//                makeAndUploadSummaryFile(sqsClient, s3, 3, "TasksResultsQ");

                break;
            } else if (isS3Message(inputMessage.body())) {
//                pool.execute(new ManagerRunner(String.valueOf("TasksQueue_" + new Date().getTime()), numOfMsgForWorker, inputMessage));

                pool.execute(new ManagerRunner("TasksQueue",
                        "TasksResultsQ", numOfMsgForWorker, inputMessage.body()));

                deleteMessageFromQ(inputMessage, sqsClient, localManagerUrl);
//                makeAndUploadSummaryFile(sqsClient, s3, 3, "TasksResultsQ");

//                executor.shutdown();
//                waitExecutorToFinish(executor);
                // TODO: 03/04/2020 Add Num of messages, maybe from LocalApp

            }
        }
         catch (IndexOutOfBoundsException ignored) {}
       }


    }

    /**
     * Terminate all running ec2 instances
     * @param ec2
     */



    private static void terminateEc2Instances(Ec2Client ec2) {
        System.out.println("enter Manager.terminateEc2Instances()");

        String nextToken = null;
        do {
            DescribeInstancesRequest dRequest = DescribeInstancesRequest.builder().nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(dRequest);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
//                    List<String> instanceIds = dRequest.instanceIds();
                    TerminateInstancesRequest request = TerminateInstancesRequest.builder().instanceIds(instance.instanceId()).build();
                    ec2.terminateInstances(request);
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
        System.out.println("exit Manager.terminateEc2Instances()");
    }


    /**
     * Make summary file from all workers results and upload to s3 bucket, named "summaryfilebucket"
     * so in order to make this work there is bucket with this name before the function run
     * @param sqs
     * @param s3
     * @param numOfMessages
     * @param tasksResultQName
     */


    private static void makeAndUploadSummaryFile(SqsClient sqs,
                                                 S3Client s3, int numOfMessages, String tasksResultQName) {
        int leftToRead = numOfMessages;
        FileWriter summaryFile = null;
        try {
           summaryFile = new FileWriter("summaryFile.html");
       }   catch (Exception ex) {
           System.out.println(ex.getMessage());
       }
        String qUrl = getQUrl(tasksResultQName, sqs);

        while (leftToRead > 0) {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(qUrl).build();
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            for (Message message : messages) {
                try {
                    assert summaryFile != null;
                    summaryFile.write(message.body() + System.getProperty("line.separator"));
                    deleteMessageFromQ(message, sqs, qUrl);
                    leftToRead--;
                }catch (IOException ex) {
                    System.out.println(ex.toString());
                }

                }
            }
        try {
            assert summaryFile != null;
            summaryFile.close();
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
        uploadFile(new File("summaryFile.html"), s3, "summaryfilebucket", "summaryFile");


    }




    /**
     * Upload first file to S3
     * @param file
     * @param s3
     * @param bucket
     * @param key
     */

    private static void uploadFile(File file, S3Client s3, String bucket, String key) {

        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .build())
                .build());


        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromFile(file));
    }


    /**
     * Waiting for some LocalApp < --- > Manager connection to finish.
     * @param executor
     */
    private static void waitExecutorToFinish(ExecutorService executor) {
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        } catch (InterruptedException ignored) {
        }
    }

//    private static void makeSummaryMessage

    private static boolean isS3Message(String inputMessage) {
        return inputMessage.substring(0, 5).equals("s3://");

    }


    private static void deleteMessageFromQ(Message message, SqsClient sqsClient, String localManagerUrl) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(localManagerUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteRequest);
    }


    // TODO: 29/03/2020 this function exists in LocalApp too.

    /**
     * @param QUEUE_NAME
     * @param sqs
     * @return this function return the Q url by its name.
     */
    private static String getQUrl(String QUEUE_NAME, SqsClient sqs) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(QUEUE_NAME)
                .build();
        //get url in order to send later
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }
}


