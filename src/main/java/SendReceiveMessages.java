import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.Date;
import java.util.List;

public class SendReceiveMessages
{
    private static final String QUEUE_NAME = "testQueue" + new Date().getTime();
 
    public static void main(String[] args)
    {
        //interface for sqs
       SqsClient sqs = SqsClient.builder().region(Region.US_WEST_2).build();
 
//        queue gets name when we create it
        try {
           CreateQueueRequest request = CreateQueueRequest.builder()
                 .queueName(QUEUE_NAME)
                 .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
           throw e;
 
        }
 
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
              .queueName(QUEUE_NAME)
              .build();
        //get url in order to send later
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();

    //choose q, and what to put there. check if we can do it by name!! if not->use s3
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody("hello world")
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);
 
 
        // Send multiple messages to the queue
        SendMessageBatchRequest send_batch_request = SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(
                        SendMessageBatchRequestEntry.builder()
                        .messageBody("Hello from message 1")
                        .id("msg_1")
                        .build()
                        ,
                        SendMessageBatchRequestEntry.builder()
                        .messageBody("Hello from message 2")
                        .delaySeconds(10)
                        .id("msg_2")
                        .build())
                .build();
        sqs.sendMessageBatch(send_batch_request);
 
        // receive messages from the queue, if empty? (maybe busy wait?)
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
              .queueUrl(queueUrl)   //which queue
              .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
 
        // delete messages from the queue, MUST.
        //maybe we should synchronized the queue
        for (Message m : messages) {
           DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                 .queueUrl(queueUrl)
                 .receiptHandle(m.receiptHandle())
                 .build();
            sqs.deleteMessage(deleteRequest);
        }
    }
}