import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
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
    private String workerOutputQName;
    private static String id;


    public ManagerRunner(String TasksQName, String workerOutputQ, int numOfMsgForWorker, String inputMessage, String id) {
        this.id = id;
        this.TasksQName = TasksQName + id;
        this.numOfMsgForWorker = numOfMsgForWorker;
        this.inputMessage = inputMessage;
        this.workerOutputQName = workerOutputQ + id;

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
                    ResponseTransformer.toFile(Paths.get("inputFile" + id + ".txt")));
        } catch (Exception ignored) {}
        finally {



            // Create SQS message for each url in the input file.

            List<String> tasks = createSqsMessages("inputFile" + id + ".txt");
            int messageCount = tasks.size();
            System.out.println("numOfMessages: " + messageCount);

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
            BuildQueueIfNotExists(sqsClient, workerOutputQName);
            System.out.println("build Workers outputQ succeed");


            createWorkers(numOfWorkers, ec2, amiId);
            System.out.println("create workers succeed");

            // Delegate Tasks to workers.
            for (String task : tasks) {
                putMessageInSqs(sqsClient, tasksQUrl, task);
            }

            System.out.println("Delegated all tasks to workers, now waiting for them to finish.." +
                    "(it sounds like a good time to lunch them!)");

//            runWorkers with the inputs: TasksQName, this.workerOutputQ



//            makeAndUploadSummaryFile(sqsClient, s3, messageCount, "Manager_Local_Queue" + id);
            System.out.println("Start making summary file.. ");
            makeAndUploadSummaryFile(sqsClient, s3, messageCount, workerOutputQName);

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
                    summaryFile.write(message.body() + '\n');
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
        System.out.println("finish making summaryFile.. start uploading summary file..");
        uploadFile(new File("summaryFile" + id + ".html"), s3,
                "dsp-private-bucket", "summaryFile");

        System.out.println("finish uploading file..put message in sqs ");
        putMessageInSqs(sqs, getQUrl("Manager_Local_Queue" + id, sqs),
                getFileUrl("dsp-private-bucket", "summaryFile"));
    }


    /**
     * Upload first file to S3
     * @param file
     * @param s3
     * @param bucket
     * @param key
     */

    private static void uploadFile(File file, S3Client s3, String bucket, String key) {

        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucket)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .build())
                    .build());
        }
        catch(BucketAlreadyExistsException ignored) {}


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
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId(amiId)
                    .instanceType(InstanceType.T2_MICRO)
                    .maxCount(numOfWorkers)
                    .minCount(numOfWorkers)
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);

        for (int index = 0; index < numOfWorkers; index++) {

            String instanceId = response.instances().get(index).instanceId();

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
                System.err.println("error while tagging an instance.. trying again");
                index--;
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
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    List<Tag> tagList = instance.tags();
                    for (Tag tag : tagList) {
                        if (!tag.value().equals("Manager") &&
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



