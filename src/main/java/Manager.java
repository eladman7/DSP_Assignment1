import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.Date;
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
        String inputMessage;
//        String sqsName = args[0];
        String sqsName = "Local_Manager_Queue";           // Save the name of the Local <--> Manager sqs
//        int numOfMsgForWorker = Integer.parseInt(args[1]);
        int numOfMsgForWorker = 2;                          // Save number of msg for each worker
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
            } catch (Exception ignored) {
            }
        }


        // Read message from Local

        //Uncomment this if u want MultiThreading
//        while (true) {
        try {
            messages = sqsClient.receiveMessage(rRLocalManager).messages();
            inputMessage = messages.get(0).body();
            if (inputMessage.equals("terminate") && messages.size() == 1) {
                executor.shutdown();
                deleteMessageFromQ(messages, sqsClient, localManagerUrl);
                try {
                    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

                } catch (InterruptedException ignored) {}
//                    break;
            }
            else /*if(isS3Message(inputMessage)){*/
                pool.execute(new ManagerRunner(String.valueOf("TasksQueue_" + new Date().getTime()), numOfMsgForWorker, inputMessage));
                deleteMessageFromQ(messages, sqsClient, localManagerUrl);

                executor.shutdown();
                try {
                    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

                } catch (InterruptedException ignored) {
                }
//            }
        } catch (IndexOutOfBoundsException ignored) {}
        finally {
//                ?putMessageInSqs(sqsClient, localManagerUrl, "Summary Message");?
        }
//       }


    }

    private static boolean isS3Message(String inputMessage) {
        return inputMessage.substring(0, 5).equals("s3://");

    }


    private static void deleteMessageFromQ(List<Message> messages, SqsClient sqsClient, String localManagerUrl) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(localManagerUrl)
                .receiptHandle(messages.get(0).receiptHandle())
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


