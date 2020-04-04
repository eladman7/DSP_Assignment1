import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

public class SQSUtils {
    private final static SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

    public static void sendMSG(String qName, String messageBody) {
        BuildQueueIfNotExists(sqs, qName);
        putMessageInSqs(sqs, getQUrl(qName, sqs), messageBody);
    }

    public static Message recieveMSG(String qName) {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(getQUrl(qName, sqs))
                .build();
        return sqs.receiveMessage(receiveRequest).messages().get(0);
    }

    public static boolean deleteMSG(Message msg, String qName) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(qName)
                .receiptHandle(msg.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
        return true;
    }

    /**
     *
     * @param sqsClient
     * @param qName
     * @return Build sqs with the name qName, if not already exists.
     */

    private static String BuildQueueIfNotExists(SqsClient sqsClient, String qName) {
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

    private static void putMessageInSqs(SqsClient sqs, String queueUrl, String message) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);
    }
}
