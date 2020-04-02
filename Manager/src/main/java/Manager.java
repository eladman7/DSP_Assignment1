import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Manager {

    public static void main(String[] args) {
        // Currently assuming there is only one LocalApplication.
        Region region = Region.US_EAST_1;
        List<Message> messages;
        String inputMessage;
//        String sqsName = args[0];
        String sqsName = "Local_Manager_Queue";           // Save the name of the Local <--> Manager sqs
//        int numOfMsgForWorker = Integer.parseInt(args[1]);
        int numOfMsgForWorker = 1;                          // Save number of msg for each worker
        SqsClient sqsClient = SqsClient.builder().region(region).build(); // Build Sqs client
        String localManagerUrl = getQUrl(sqsName, sqsClient);
        ReceiveMessageRequest rRLocalManager;
        ExecutorService executor = Executors.newCachedThreadPool();
        ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;


        //connect to the Queue
        while (true) {
            try {
                rRLocalManager = ReceiveMessageRequest.builder()
                        .queueUrl(localManagerUrl)   //which queue
                        .build();
                break;
            } catch (Exception ignored) {}
        }


        // Read message from Local
        while (true) {
            try {
                messages = sqsClient.receiveMessage(rRLocalManager).messages();
                inputMessage = messages.get(0).body();
                if (inputMessage.equals("terminate") && messages.size() == 1) {
                    executor.shutdown();
                    deleteMessageFromQ(messages, sqsClient, localManagerUrl);
                    try {
                        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

                    }catch (InterruptedException ignored) {}
                        break;
                }
                else {
                    pool.execute(new ManagerRunner(String.valueOf("TasksQueue_" + new Date().getTime()), numOfMsgForWorker, inputMessage));
                    deleteMessageFromQ(messages, sqsClient, localManagerUrl);
                }
            } catch (IndexOutOfBoundsException ignored) {}
            finally {
                putMessageInSqs(sqsClient, localManagerUrl, "Summary Message");
            }
        }







    }

    private static void deleteMessageFromQ(List<Message> messages, SqsClient sqsClient, String localManagerUrl) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(localManagerUrl)
                .receiptHandle(messages.get(0).receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteRequest);
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
    // TODO: 30/03/2020 synchronized?
    public static synchronized int numOfRunningWorkers(Ec2Client ec2) {
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

}


/*        while (true) {
            try {
                rRLocalManager = ReceiveMessageRequest.builder()
                        .queueUrl(localManagerUrl)   //which queue
                        .build();
                break;
            } catch (Exception ex) {}
        }


        List<Message> messages;
        String inputMessage;

        // TODO: 29/03/2020 change to something prettier
        //busy wait..
        while (true) {
            try {
                messages = sqsClient.receiveMessage(rRLocalManager).messages();
                inputMessage = messages.get(0).body();
                break;
            } catch (IndexOutOfBoundsException ignored) {}
        }

        if (inputMessage.equals("terminate")) {
            System.out.println("waiting from all workers to finish.. ");
            // TODO: 30/03/2020 add waits for all the worker to finish, create response and terminate.

        } else {

            String inputBucket = extractBucket(inputMessage);
            String inputKey = extractKey(inputMessage);
            s3.getObject(GetObjectRequest.builder().bucket(inputBucket).key(inputKey).build(), //download input file
                    ResponseTransformer.toFile(Paths.get("inputFile.txt")));
            // TODO: 29/03/2020 continue from the 4th square at the assignment page -  "The Manager"
            List<String> tasks = createSqsMessages("inputFile.txt");
            int messageCount = tasks.size();
            int numOfRunningWorkers = numOfRunningWorkers(ec2);

            if (numOfRunningWorkers == 0) {
                numOfWorkers = messageCount/numOfMsgForWorker;      // Number of workers the job require.
            } else numOfWorkers = (messageCount/numOfMsgForWorker) - numOfRunningWorkers;

            createQByName("TasksQueue", sqsClient);
            String managerWorkersUrl = getQUrl("TasksQueue", sqsClient);
            createWorkers(numOfWorkers, ec2, amiId);

            for (String task: tasks) {
                putMessageInSqs(sqsClient, managerWorkersUrl, task);
            }




        }*/
