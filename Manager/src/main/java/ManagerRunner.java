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

import java.io.*;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ManagerRunner implements Runnable {
    private String TasksQName;
    private int numOfMsgForWorker;
    private String inputMessage;
    private String workerOutputQ;
    private static String id = "";

    // TODO: 04/04/2020 add id to constructor

    public ManagerRunner(String TasksQName, String workerOutputQ, int numOfMsgForWorker, String inputMessage) {
        this.TasksQName = TasksQName;
        this.numOfMsgForWorker = numOfMsgForWorker;
        this.inputMessage = inputMessage;
        this.workerOutputQ = workerOutputQ;

    }


    @Override
    public void run() {

        S3Client s3;
        Region region = Region.US_EAST_1;
        String amiId = "ami-076515f20540e6e0b";
        Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
        s3 = S3Client.builder().region(region).build();
        SqsClient sqsClient = SqsClient.builder().region(region).build(); // Build Sqs client


        String inputBucket = extractBucket(inputMessage);
        String inputKey = extractKey(inputMessage);

        //download input file, Save under "inputFile.txt"

        try {
            s3.getObject(GetObjectRequest.builder().bucket(inputBucket).key(inputKey).build(),
                    ResponseTransformer.toFile(Paths.get("inputFile.txt")));
        } catch (Exception ignored) {}
        finally {



            // Create SQS message for each url in the input file.

            List<String> tasks = createSqsMessages("inputFile.txt");
            int messageCount = tasks.size();

            int numOfRunningWorkers = numOfRunningWorkers(ec2);
            int numOfWorkers = 0;

            if (numOfRunningWorkers == 0) {
                numOfWorkers = messageCount / numOfMsgForWorker;      // Number of workers the job require.
            } else numOfWorkers = (messageCount / numOfMsgForWorker) - numOfRunningWorkers;


            String tasksQUrl;
            // Build Tasks Q name
            tasksQUrl = BuildQueueIfNotExists(sqsClient, TasksQName);
            System.out.println("build TasksQ succeed");


            // Build Workers output Q
            BuildQueueIfNotExists(sqsClient, workerOutputQ);
            System.out.println("build Workers outputQ succeed");


            createWorkers(numOfWorkers, ec2, amiId);
            System.out.println("create workers succeed");

            // Delegate Tasks to workers.
            for (String task : tasks) {
                putMessageInSqs(sqsClient, tasksQUrl, task);
            }

            System.out.println("Delegated all tasks to workers, now waiting for them to finish.." +
                    "(it sounds like a good time to lunch them!)");



//            makeAndUploadSummaryFile(sqsClient, s3, messageCount, "Manager_Local_Queue" + id);
            makeAndUploadSummaryFile(sqsClient, s3, messageCount, "TasksResultsQ");

            System.out.println("finish make and upload summary file, exit ManagerRunner");




        }


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
            summaryFile = new FileWriter("summaryFile" + id + ".html");
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
        uploadFile(new File("summaryFile" + id + ".html"), s3, "summaryfilebucket" + id, "test");
        putMessageInSqs(sqs, getQUrl("Manager_Local_Queue", sqs), getFileUrl("summaryfilebucket", "test"));
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
     *
     * @param sqsClient
     * @param qName
     * @return Build sqs with the name qName, if not already exists.
     */

    private String BuildQueueIfNotExists(SqsClient sqsClient, String qName) {
        String tasksQUrl;
        try {
            tasksQUrl = getQUrl(qName, sqsClient);
            // Throw exception in the first try
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
     * This function create numOfWorker Ec2-workers.
     * @param numOfWorkers
     * @param ec2
     * @param amiId
     */
    public static void createWorkers (int numOfWorkers, Ec2Client ec2, String amiId) {
        for (int index = 0; index < numOfWorkers; index++) {
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
                    .value("WorkerNumber " + index)
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
    }


    /**
     * @param ec2
     * @return the number of currently running client
     */

    public static int numOfRunningWorkers(Ec2Client ec2) {
        String nextToken = null;
        int counter = 0;
        do {
            // TODO: 30/03/2020 check without maxResults
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    List<Tag> tagList = instance.tags();
                    for (Tag tag : tagList) {
                        if (!tag.value().equals("manager") &&
                                (instance.state().name().toString().equals("running") ||
                                        instance.state().name().toString().equals("pending")))
                            counter++;
                    }
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);

        return counter;
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
     * @param filename
     * @return List of all the messages from the pdf file, which we get by sqs.
     */
    public static List<String> createSqsMessages(String filename) {
        List<String> tasks = new LinkedList<>();
        BufferedReader reader;
        String line;

        try {
            reader = new BufferedReader(new FileReader(filename));
            line = reader.readLine();
            while (line != null) {
                tasks.add(line);
                line = reader.readLine();

            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return tasks;
    }


    private static void deleteMessageFromQ(Message message, SqsClient sqsClient, String localManagerUrl) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(localManagerUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteRequest);
    }
    private static String getFileUrl (String bucket, String key) {
        return "s3://" + bucket + "/" + key;
    }


}



