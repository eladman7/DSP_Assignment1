import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Manager {

    public static void main(String[] args) {
        // Currently assuming there is only one LocalApplication.
        S3Client s3;
        Region region = Region.US_EAST_1;
        String amiId = "ami-b66ed3de";
        String ec2NameManager = "manager";

        String inputFileName = args[0];     // Save the input file name, get it from LocalApp
        String sqsName = args[1];           // Save the name of the Local <--> Manager sqs
        int numOfMsgForWorker = Integer.parseInt(args[2]);  // Save number of msg for each worker
        int numOfWorkers = 0;
        s3 = S3Client.builder().region(region).build();
        SqsClient sqs = SqsClient.builder().region(region).build(); // Build Sqs client
        String qUrl = getQUrl(sqsName, sqs);
        Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();



        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(qUrl)   //which queue
                .build();
        List<Message> messages;
        String inputMessage;

        // TODO: 29/03/2020 change to something prettier
        //busy wait..
        while (true) {
            try {
                messages = sqs.receiveMessage(receiveRequest).messages();
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
            createWorkers(numOfWorkers, ec2, amiId);




        }

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
