import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.utils.CollectionUtils;

import java.util.List;

public class SQSUtils {
    private final static SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

    public static void sendMSG(String qName, String messageBody) {
        System.out.println("inside SQSUtils.sendMSG()");
        createQByName(qName);
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
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
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
        sqs.sendMessage(send_msg_request);
    }
}
