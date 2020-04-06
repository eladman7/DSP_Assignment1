import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalApplication {


    public static void main(String[] args) {

        final String localAppId = String.valueOf(System.currentTimeMillis());
        String input_file_path = args[0];
        int numOfPdfPerWorker = Integer.parseInt(args[1]);
        String terMessage = args[2];
        String inputFileKey = "inputFile" + localAppId;
        String bucket = "dsp-private-bucket" + System.currentTimeMillis();
        Region region = Region.US_EAST_1;
        String amiId = "ami-076515f20540e6e0b";
        String ec2NameManager = "Manager";


        // ---- Upload input file to s3 ----
        S3Utils.uploadFile(input_file_path, inputFileKey, bucket);      // Upload input File to S3
        System.out.println("success upload input file");

        // ---- Upload first message to sqs


        String LocalManagerQName = "Local_Manager_Queue";
        SqsClient sqs = SqsClient.builder().region(region).build(); // Build Sqs client
        BuildQueueIfNotExists(LocalManagerQName, sqs);                             // Creat Q
        String localManagerQUrl = getQUrl(LocalManagerQName, sqs);
        String fileUrl = getFileUrl(bucket, inputFileKey);
        putMessageInSqs(sqs, localManagerQUrl, fileUrl);


        System.out.println("success uploading first message to sqs");

        // ---- Create Manager Instance


        Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
        if (!isManagerRunning(ec2)) {
            System.out.println("There is no manager running.. lunch manager");
            // Run manager JarFile with input : numOfPdfPerWorker.
            createEc2Instance(ec2, amiId, ec2NameManager);
            System.out.println("Success lunching manager");
        } else System.out.println("Ec2 manager already running.. ");


        // ---- Read SQS summary message from manager

        String ManagerLocalQName = "Manager_Local_Queue" + localAppId;
        BuildQueueIfNotExists(ManagerLocalQName, sqs);
        String managerLocalQUrl = getQUrl(ManagerLocalQName, sqs);

        // receive messages from the queue, if empty? (maybe busy wait?)
        System.out.println("waiting for a summary file from manager..");
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(managerLocalQUrl)   //which queue
                .build();
        List<Message> messages;
        Message sMessage;
        String summaryMessage;

        //busy wait..
        while (true) {
            try {
                messages = sqs.receiveMessage(receiveRequest).messages();
                sMessage = messages.get(0);
                summaryMessage = sMessage.body();
                if (summaryMessage != null)
                    break;
            } catch (IndexOutOfBoundsException ignored) {
            }
        }
        deleteMessageFromQ(sMessage, sqs, managerLocalQUrl);

        System.out.println("local app gets its summary file.. download and sent termination message if needed");


        // TODO: 05/04/2020 check for the key
        //Download summary file and create Html output
        String summaryBucket = extractBucket(summaryMessage);
        String summaryKey = extractKey(summaryMessage);

        try {
            S3Utils.getObjectToLocal(summaryKey, summaryBucket, "summaryFile" + localAppId + ".html");
        } catch (Exception ignored) {
            //send termination message if needed
        } finally {
            sendTerminationMessageIfNeeded(terMessage, sqs, localManagerQUrl);
        }

        System.out.println("Local sent terminate message and finish.. Bye");

        // TODO: 05/04/2020 Add delete bucket and Queues which there are no used for them.

    }

    private static void sendTerminationMessageIfNeeded(String terMessage, SqsClient sqs, String queueUrl) {
        if (terMessage.equals("terminate")) {
            putMessageInSqs(sqs, queueUrl, "terminate");
        }
    }


    /**
     * Delete message from Q.
     * @param message
     * @param sqsClient
     * @param localManagerUrl
     */

    private static void deleteMessageFromQ(Message message, SqsClient sqsClient, String localManagerUrl) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(localManagerUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteRequest);
    }


    /**
     * @param body
     * @return the bucket name from a sqs message
     */
    public static String extractBucket(String body) {
        Pattern pattern = Pattern.compile("//(.*?)/((.+?)*)");
        Matcher matcher = pattern.matcher(body);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return " ";
    }

    /**
     * @param body
     * @return the key from a sqs message
     */
    public static String extractKey(String body) {
        Pattern pattern = Pattern.compile("//(.*?)/((.+?)*)");
        Matcher matcher = pattern.matcher(body);
        if (matcher.find()) {

            return matcher.group(2);
        }
        return " ";
    }

    /**
     * @param ec2
     * @return true iff the manager running
     */
    public static boolean isManagerRunning(Ec2Client ec2) {
        String nextToken = null;
        do {
//                     DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    List<Tag> tagList = instance.tags();
                    for (Tag tag : tagList) {
                        if (tag.value().equals("manager") &&
                                (instance.state().name().toString().equals("running") ||
                                        instance.state().name().toString().equals("pending")))
                            return true;
                    }
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);

        return false;
    }

    /**
     * create an Ec2 instance
     * @param ec2
     * @param amiId
     * @param ec2Name
     */

    private static void createEc2Instance(Ec2Client ec2, String amiId, String ec2Name) {
        String bucketName = "managercode" + System.currentTimeMillis(); //dsp-private-bucket
        String fileKey = "managerapp";
        S3Utils.uploadFile("/Users/eman/IdeaProjects/DSP-Assignment1/out/artifacts/Manager_jar/Manager.jar",
                fileKey, bucketName);
        String s3Path = "https://" + bucketName + ".s3.amazonaws.com/" + fileKey;
        String script = "#!/bin/bash\nwget " + s3Path + " -O manager.jar\n" + "java -jar manager.jar\n";
        System.out.println("user data: " + script);
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                .keyName("eladkey")
                .build();
        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();
        Tag tag = Tag.builder()
                .key("name")
                .value(ec2Name)
                .build();
        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();
        try {
            ec2.createTags(tagRequest);
            System.out.println(
                    "Successfully started EC2 instance: " + instanceId + "based on AMI: " + amiId);
        } catch (Ec2Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    /**
     * put message in sqs with the url queueUrl
     * @param sqs
     * @param queueUrl
     * @param message
     */

    private static void putMessageInSqs(SqsClient sqs, String queueUrl, String message) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);
    }

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


    /**
     * @param sqsClient
     * @param qName
     * @return Build sqs with the name qName, if not already exists.
     */

    private static String BuildQueueIfNotExists(String qName, SqsClient sqsClient) {
        String tasksQUrl;
        try {
            tasksQUrl = getQUrl(qName, sqsClient);
        } catch (Exception ex) {
            createQByName(qName, sqsClient);
            tasksQUrl = getQUrl(qName, sqsClient);
        }
        return tasksQUrl;
    }

    /**
     * create a sqs queue with the name QUEUE_NAME, using sqs client
     * @param QUEUE_NAME
     * @param sqs
     */

    private static void createQByName(String QUEUE_NAME, SqsClient sqs) {
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(QUEUE_NAME)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            System.out.println("catch exception: " + e.toString());

        }
    }


    /**
     * Upload first Input file to S3
     * @param input_file
     * @param s3
     * @param bucket
     * @param key
     */

    private static void uploadInputFile(File input_file, S3Client s3, String bucket, String key) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .build())
                .build());
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromFile(input_file));
    }


    /**
     * Extract the file url from some s3 path
     * @param bucket
     * @param key
     * @return
     */
    private static String getFileUrl(String bucket, String key) {
        return "s3://" + bucket + "/" + key;
    }

}
