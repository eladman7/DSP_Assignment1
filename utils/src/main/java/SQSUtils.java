import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

public class SQSUtils {
    private final static SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

    public static void sendMSG(String qName, String messageBody) {
        System.out.println("inside SQSUtils.sendMSG()");
        BuildQueueIfNotExists(qName);
        putMessageInSqs(getQUrl(qName), messageBody);
    }

    public static Message recieveMSG(String qName) {
        return recieveMSG(qName, 0);
    }

    public static Message recieveMSG(String qName, int waitTime) {
//        System.out.println("inside SQSUtils.recieveMSG()");
        ReceiveMessageRequest receiveRequest = getReceiveMessageRequest(qName, waitTime, 1);
        List<Message> messages = null;
        //adding try and catch for cloud reasons.. so it'll keep on running.
        try {
            messages = sqs.receiveMessage(receiveRequest).messages();
        } catch (SqsException ignored) {
        } //there is already some handler for null case
        return CollectionUtils.isNullOrEmpty(messages) ? null : messages.get(0);
    }

    public static List<Message> recieveMessages(String qName, int waitTime, int maxCount) {
        System.out.println("inside SQSUtils.recieveMSG()");
        ReceiveMessageRequest receiveRequest = getReceiveMessageRequest(qName, waitTime, maxCount);
        List<Message> messages = null;
        //adding try and catch for cloud reasons.. so it'll keep on running.
        try {
            messages = sqs.receiveMessage(receiveRequest).messages();
        } catch (SqsException ignored) {
        } //there is already some handler for null case
        return CollectionUtils.isNullOrEmpty(messages) ? new ArrayList<>() : messages;
    }

    private static ReceiveMessageRequest getReceiveMessageRequest(String qName, int waitTime, int maxCount) {
        return ReceiveMessageRequest.builder()
                .queueUrl(getQUrl(qName))
                .maxNumberOfMessages(maxCount)
                .waitTimeSeconds(waitTime)
                .build();
    }

    public static boolean deleteMSG(Message msg, String qName) {
        System.out.println("inside SQSUtils.deleteMSG()");
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(getQUrl(qName))
                .receiptHandle(msg.receiptHandle())
                .build();
        sqs.deleteMessage(deleteRequest);
        return true;
    }

    /**
     * @param qName
     * @return Build sqs with the name qName, if not already exists.
     */

    public static String BuildQueueIfNotExists(String qName) {
        String tasksQUrl;
        try {
            tasksQUrl = getQUrl(qName);
            // Throw exception in the first try
        } catch (QueueDoesNotExistException ex) {
            createQByName(qName);
            tasksQUrl = getQUrl(qName);
        }
        return tasksQUrl;
    }

    private static void createQByName(String QUEUE_NAME) {
        CreateQueueRequest request = CreateQueueRequest.builder()
                .queueName(QUEUE_NAME)
                .build();
        CreateQueueResponse create_result = sqs.createQueue(request);
    }

    /**
     * @param QUEUE_NAME
     * @return this function return the Q url by its name.
     */
    private static String getQUrl(String QUEUE_NAME) throws QueueDoesNotExistException {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(QUEUE_NAME)
                .build();
        //get url in order to send later
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }

    private static void putMessageInSqs(String queueUrl, String message) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .delaySeconds(5)
                .build();
        //added for cloud reasons.
        try {
            sqs.sendMessage(send_msg_request);
        } catch (Exception ex) {
            System.out.println("Exception at SQSUtils.putMessageInSqs: " + ex);
        }
    }

    public static void deleteQ(String qName) {
        DeleteQueueRequest deleteManLocQ = DeleteQueueRequest.builder().queueUrl(getQUrl(qName)).build();
        sqs.deleteQueue(deleteManLocQ);
    }
}
