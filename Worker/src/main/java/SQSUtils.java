import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.utils.CollectionUtils;

import java.util.List;

public class SQSUtils {
    private final static SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

    public static void sendMSG(String qName, String messageBody) {
        System.out.println("inside SQSUtils.sendMSG()");
        BuildQueueIfNotExists(sqs, qName);
        putMessageInSqs(getQUrl(qName), messageBody);
    }

    public static Message recieveMSG(String qName) {
        return recieveMSG(qName, 0);
    }

    public static Message recieveMSG(String qName, int waitTime) {
        System.out.println("inside SQSUtils.recieveMSG()");
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(getQUrl(qName))
                .maxNumberOfMessages(1)
                .waitTimeSeconds(waitTime)
                .build();
        List<Message> messages = null;
        //adding try and catch for cloud reasons.. so it'll keep on running.
        try {
          messages = sqs.receiveMessage(receiveRequest).messages();
        } catch (SqsException ignored) {} //there is already some handler for null case
        return CollectionUtils.isNullOrEmpty(messages) ? null : messages.get(0);
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
     *
     * @param sqsClient
     * @param qName
     * @return Build sqs with the name qName, if not already exists.
     */

    private static String BuildQueueIfNotExists(SqsClient sqsClient, String qName) {
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
     * @return this function return the Q url by its name.
     */
    private static String getQUrl(String QUEUE_NAME) {
        String queueUrl = "";
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(QUEUE_NAME)
                .build();
        //get url in order to send later
        try {
            queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        }
        catch (QueueDoesNotExistException ignored) {
            System.out.println("Exception ignored in Worker.getQUrl():\n" + ignored);
        }
        return queueUrl;
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
            System.out.println("Exception at SQSUtils.putMessageInSqs: " + ex);}
    }
}
