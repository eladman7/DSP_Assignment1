import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalApplication {


    public static void main(String[]args) {

        String input_file_path = args[0];
        int numOfPdfPerWorker = Integer.parseInt(args[1]);
        String terMessage = args[2];
        File input_file = new File(input_file_path);
        String inputFileKey = "inputFile";
        String bucket = "bucket" + System.currentTimeMillis();
        S3Client s3;
        Region region = Region.US_EAST_1;
        String amiId = "ami-076515f20540e6e0b";
        String ec2NameManager = "manager";


        // ---- Upload input file to s3 ----


        s3 = S3Client.builder().region(region).build();             // Build S3 client
        uploadInputFile(input_file, s3, bucket, inputFileKey);      // Upload input File to S3
        System.out.println("success upload input file");

        // ---- Upload first message to sqs

//        String LocalManagerQName = "Local_Manager_Queue" + new Date().getTime();

        String LocalManagerQName = "Local_Manager_Queue";
        SqsClient sqs = SqsClient.builder().region(region).build(); // Build Sqs client
        createQByName(LocalManagerQName, sqs);                             // Creat Q
        String queueUrl = getQUrl(LocalManagerQName, sqs);
        String fileUrl = getFileUrl(bucket, inputFileKey);
        putMessageInSqs(sqs, queueUrl, fileUrl);
        System.out.println("success uploading first message to sqs");

        // ---- Create Manager Instance


        Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
        if (!isManagerRunning(ec2)) {
            System.out.println("There is no manager running.. lunch manager");
            createEc2Instance(ec2, amiId, ec2NameManager);
            System.out.println("Success lunching manager");
        } else System.out.println("Ec2 manager already running.. ");

        // Some time wasting..
        int i = 0;
        while (i < 1000) {
            i++;
        }
//        putMessageInSqs(sqs, queueUrl, "terminate");

        // STOP HERE IF YOU WANT TO TEST LOCAL <--> WORKER CONNECTION


        // ---- Read SQS summary message from manager

        // receive messages from the queue, if empty? (maybe busy wait?)
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)   //which queue
                .build();
        List<Message> messages;
        String summaryMessage;
        //busy wait..
        while (true) {
            try {
                messages = sqs.receiveMessage(receiveRequest).messages();
                summaryMessage = messages.get(0).body();
                break;
            } catch (IndexOutOfBoundsException ignored) {}
        }

        //Download summary file and create Html output
        String summaryBucket = extractBucket(summaryMessage);
        String summaryKey = extractKey(summaryMessage);

        try {
            s3.getObject(GetObjectRequest.builder().bucket(summaryBucket).key(summaryKey).build(),
                    ResponseTransformer.toFile(Paths.get("summaryFile.html")));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {

            if (args[2].equals("terminate")) {
                String localManagerUrl = getQUrl(LocalManagerQName, sqs);
                putMessageInSqs(sqs, localManagerUrl, "terminate");
            }
        }

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
                    DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
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

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
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

// TODO: 29/03/2020 check if there is another way.

    /**
     * Extract the file url from some s3 path
     * @param bucket
     * @param key
     * @return
     */
    private static String getFileUrl (String bucket, String key) {
        return "s3://" + bucket + "/" + key;
    }

}
