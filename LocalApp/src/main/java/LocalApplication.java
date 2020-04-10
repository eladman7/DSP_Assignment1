import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalApplication {

    private static final String PRIVATE_BUCKET = "dsp-private-bucket";

    public static void main(String[] args) {

        final String localAppId = String.valueOf(System.currentTimeMillis());
        String input_file_path = args[0];
        int numOfPdfPerWorker = Integer.parseInt(args[1]);

        String terMessage = checkForTerminateMessage(args);

        String inputFileKey = "inputFile" + localAppId;
        Region region = Region.US_EAST_1;
        String amiId = "ami-076515f20540e6e0b";

        // ---- Upload input file to s3 ----
        S3Utils.uploadFile(input_file_path, inputFileKey, PRIVATE_BUCKET, false);      // Upload input File to S3
        System.out.println("success upload input file");

        // ---- Upload first message to sqs
        String LocalManagerQName = "Local_Manager_Queue";
        SqsClient sqs = SqsClient.builder().region(region).build(); // Build Sqs client
        BuildQueueIfNotExists(LocalManagerQName, sqs);                             // Creat Q
        String localManagerQUrl = getQUrl(LocalManagerQName, sqs);
        String fileUrl = getFileUrl(PRIVATE_BUCKET, inputFileKey);
        putMessageInSqs(sqs, localManagerQUrl, fileUrl);

        System.out.println("success uploading first message to sqs");

        // ---- Create Manager Instance
        if (!EC2Utils.isManagerRunning()) {
            System.out.println("There is no running manager.. lunch manager");
            // Run manager JarFile with input : numOfPdfPerWorker.
            String managerScript = createManagerUserData(numOfPdfPerWorker);
            EC2Utils.createEc2Instance(amiId, "Manager", managerScript, 1);
//            EC2Utils.createEc2Instance(amiId, "Manager", "", 1);

            System.out.println("Success lunching manager");
        } else System.out.println("Ec2 manager already running.. ");


        // ---- Read SQS summary message from manager
        System.out.println("building manager < -- > local queue");
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

        System.out.println("Local sent terminate message and finish..deleting local Q's.. Bye");
        deleteLocalAppQueues(localAppId, sqs);

    }

    /**
     * check if the user insert terminate message
     * @param args
     * @return
     */

    private static String checkForTerminateMessage(String[] args) {
        String terMessage = "";
        if (args.length > 2)
            terMessage = args[2];
        return terMessage;
    }

    // TODO: 07/04/2020 connect num of msg per worker to here
    private static String createManagerUserData(int numOfPdfPerWorker) {
        String bucketName = PRIVATE_BUCKET;
        String fileKey = "managerapp";
        System.out.println("Uploading manager jar..");
        S3Utils.uploadFile("/home/bar/IdeaProjects/Assignment1/out/artifacts/Manager_jar/Manager.jar",
                fileKey, bucketName, false);

        try {
            System.out.println("Uploading worker jar..");
            S3Utils.uploadFile("/home/bar/IdeaProjects/Assignment1/out/artifacts/Worker_jar/Worker.jar",
                    "workerapp", bucketName, false);
        } catch (Exception ex) {
            System.out.println(
                    "in LocalApplication.createManagerUserData: " + ex.getMessage());
        }

        String s3Path = "https://" + bucketName + ".s3.amazonaws.com/" + fileKey;
        String script = "#!/bin/bash\n"
                + "wget " + s3Path + " -O /home/ec2-user/Manager.jar\n" +
                "java -jar /home/ec2-user/Manager.jar " + numOfPdfPerWorker + "\n";
        System.out.println("user data: " + script);
        return script;
    }

    private static void deleteLocalAppQueues(String localAppId, SqsClient sqs) {

        DeleteQueueRequest deleteManLocQ = DeleteQueueRequest.builder().
                queueUrl(getQUrl("Manager_Local_Queue" + localAppId, sqs)).build();
        DeleteQueueRequest deleteTasksQ = DeleteQueueRequest.builder().
                queueUrl(getQUrl("TasksQueue", sqs)).build();
        DeleteQueueRequest deleteTasksResultsQ = DeleteQueueRequest.builder().
                queueUrl(getQUrl("TasksResultsQ" + localAppId , sqs)).build();
        try {
            sqs.deleteQueue(deleteManLocQ);
            sqs.deleteQueue(deleteTasksQ);
            sqs.deleteQueue(deleteTasksResultsQ);
        } catch (Exception ex) {
            System.out.println("There is some problem in deleting local app Q's");
        }
    }

    private static void sendTerminationMessageIfNeeded(String terMessage, SqsClient sqs, String queueUrl) {
        if (terMessage.equals("terminate")) {
            putMessageInSqs(sqs, queueUrl, "terminate");
        }
    }


    /**
     * Delete message from Q.
     *
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
     * put message in sqs with the url queueUrl
     *
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
     *
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
     *
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
     *
     * @param bucket
     * @param key
     * @return
     */
    private static String getFileUrl(String bucket, String key) {
        return "s3://" + bucket + "/" + key;
    }

}
