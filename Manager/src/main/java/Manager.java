import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

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
        final String sqsName = "Local_Manager_Queue";           // Save the name of the Local <--> Manager sqs
        int numOfMsgForWorker = Integer.parseInt(args[0]);// Save number of msg for each worker
        System.out.println("manager: setting num of messages per worker to: " + numOfMsgForWorker);
        SqsClient sqsClient = SqsClient.builder().region(Region.US_EAST_1).build(); // Build Sqs client

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
        List<Message> messages;
        Message inputMessage;
        while (true) {
            try {
                messages = sqsClient.receiveMessage(rRLocalManager).messages();
                inputMessage = messages.get(0);
                // Terminate stay as is, only one send terminate and we done with this.
                if (inputMessage.body().equals("terminate") && messages.size() == 1) {
                    System.out.println("manager get terminate message, deleting terminate message");

                    deleteMessageFromQ(inputMessage, sqsClient, localManagerUrl);
                    System.out.println("waiting for all local apps connections to finish");

                    executor.shutdown();
                    waitExecutorToFinish(executor);
                    System.out.println("terminating ec2 instances.. ");

                    EC2Utils.terminateEc2Instances();
                    System.out.println("succeed terminate all ec2 instances, quiting.. Bye");

                    break;
                } else if (isS3Message(inputMessage.body())) {
                    pool.execute(new ManagerRunner("TasksQueue",
                            "TasksResultsQ", numOfMsgForWorker, inputMessage.body(), extractId(inputMessage.body())));

                    deleteMessageFromQ(inputMessage, sqsClient, localManagerUrl);

                }
            } catch (IndexOutOfBoundsException ignored) {
                System.out.println(ignored);
            }
        }

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
     * @param messagePath
     * @return the key from a sqs message
     */
    public static String extractId(String messagePath) {
        Pattern pattern = Pattern.compile("(.*?)inputFile((.+?)*)");
        Matcher matcher = pattern.matcher(messagePath);
        if (matcher.find()) {

            return matcher.group(2);
        }
        return " ";
    }
}


