import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ManagerRunner implements Runnable {
    private final String TasksQName;
    private final int numOfMsgForWorker;
    private final String inputMessage;
    private final String workerOutputQName;
    private static String id;


    public ManagerRunner(String TasksQName, String workerOutputQ, int numOfMsgForWorker, String inputMessage, String id) {
        this.id = id;
        this.TasksQName = TasksQName;
        this.numOfMsgForWorker = numOfMsgForWorker;
        this.inputMessage = inputMessage;
        this.workerOutputQName = workerOutputQ + id;
    }


    @Override
    public void run() {
        String amiId = "ami-076515f20540e6e0b";
        SqsClient sqsClient = SqsClient.builder().region(Region.US_EAST_1).build(); // Build Sqs client

        String inputBucket = extractBucket(inputMessage);
        String inputKey = extractKey(inputMessage);

        //download input file, Save under "inputFile.txt"
        try {
            S3Utils.getObjectToLocal(inputKey, inputBucket, "inputFile" + id + ".txt");
        } catch (Exception ignored) {
        } finally {

            // Create SQS message for each url in the input file.
            List<String> tasks = createSqsMessages("inputFile" + id + ".txt");
            int messageCount = tasks.size();
            System.out.println("numOfMessages: " + messageCount);

            // TODO: 10/04/2020 add synchronization somehow
            int numOfRunningWorkers = EC2Utils.numOfRunningWorkers();

            int numOfWorkers;
            if (numOfRunningWorkers == 0) {
                numOfWorkers = messageCount / numOfMsgForWorker;      // Number of workers the job require.
            } else numOfWorkers = (messageCount / numOfMsgForWorker) - numOfRunningWorkers;

            String tasksQUrl;
            // Build Tasks Q name
            // TODO: 10/04/2020 stop this stupid check after the first time.
            tasksQUrl = BuildQueueIfNotExists(sqsClient, TasksQName);
            System.out.println("build TasksQ succeed");


            // Build Workers output Q
            BuildQueueIfNotExists(sqsClient, workerOutputQName);
            System.out.println("build Workers outputQ succeed");

            // Delegate Tasks to workers.
            for (String task : tasks) {
                putMessageInSqs(sqsClient, tasksQUrl, task);
            }

            System.out.println("Delegated all tasks to workers, now waiting for them to finish.." +
                    "(it sounds like a good time to lunch them!)");
            //assert there are no more than 10 workers running.
            if(numOfWorkers + numOfRunningWorkers < 10) {
                createWorkers(numOfWorkers, amiId);
            }
            else {
                System.out.println("tried to build more than 10 instances.. exit..");
                return;
            }
            System.out.println("create workers succeed");



//            runWorkers with the inputs: TasksQName, this.workerOutputQ
//            makeAndUploadSummaryFile(sqsClient, s3, messageCount, "Manager_Local_Queue" + id);
            System.out.println("Start making summary file.. ");
            makeAndUploadSummaryFile(sqsClient, messageCount, workerOutputQName);

            System.out.println("finish make and upload summary file, exit ManagerRunner");


        }


    }


    /**
     * Make summary file from all workers results and upload to s3 bucket, named "summaryfilebucket"
     * so in order to make this work there is bucket with this name before the function run
     * @param sqs
     * @param numOfMessages
     * @param tasksResultQName
     */
    private void makeAndUploadSummaryFile(SqsClient sqs, int numOfMessages, String tasksResultQName) {
        int leftToRead = numOfMessages;
        FileWriter summaryFile = null;
        try {
            summaryFile = new FileWriter("summaryFile" + id + ".html");
        } catch (Exception ex) {
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
                } catch (IOException ex) {
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
        System.out.println("RunInstancesResponse responsefinish making summaryFile.. start uploading summary file..");
        S3Utils.uploadFile("summaryFile" + id + ".html",
                "summaryFile", "dsp-private-bucket", true);

        System.out.println("finish uploading file..put message in sqs ");
        putMessageInSqs(sqs, getQUrl("Manager_Local_Queue" + id, sqs),
                getFileUrl("dsp-private-bucket", "summaryFile"));
    }


    /**
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

    private void createQByName(String QUEUE_NAME, SqsClient sqs) {
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
    private void putMessageInSqs(SqsClient sqs, String queueUrl, String message) {
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
     * @param amiId
     */
    public void createWorkers(int numOfWorkers, String amiId) {
        String[] instancesNames = new String[numOfWorkers];
        for (int i = 0; i < numOfWorkers; i++){
            instancesNames[i] = "WorkerNumber" + i;
        }
        EC2Utils.createEc2Instance(amiId, instancesNames, createWorkerUserData(), numOfWorkers);
//        EC2Utils.createEc2Instance(amiId, instancesNames, "", numOfWorkers);

    }

    private String createWorkerUserData() {
        String bucketName = "dsp-private-bucket";
        String fileKey = "workerapp";
        String s3Path = "https://" + bucketName + ".s3.amazonaws.com/" + fileKey;
        String script = "#!/bin/bash\n"
                + "wget " + s3Path + " -O /home/ec2-user/worker.jar\n" +
                "java -jar /home/ec2-user/worker.jar " + this.TasksQName + " " + this.workerOutputQName + "\n";
        System.out.println("user data: " + script);
        return script;
    }

    /**
     * @param QUEUE_NAME
     * @param sqs
     * @return this function return the Q url by its name.
     */
    private String getQUrl(String QUEUE_NAME, SqsClient sqs) {
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
    public String extractBucket(String body) {
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
    public String extractKey(String body) {
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
    public List<String> createSqsMessages(String filename) {
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


    private void deleteMessageFromQ(Message message, SqsClient sqsClient, String localManagerUrl) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(localManagerUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteRequest);
    }

    private String getFileUrl(String bucket, String key) {
        return "s3://" + bucket + "/" + key;
    }


}



