import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.util.Date;

public class LocalApplication {


    public static void main(String[]args) {

        String input_file_path = args[0];
        int numOfPdfPerWorker = Integer.parseInt(args[1]);
        String terMessage = args[2];
        File input_file = new File(input_file_path);
        String inputFileKey = "inputFile";
        String bucket = "bucket" + System.currentTimeMillis();
        S3Client s3;
        Region region = Region.US_EAST_1;


        // ---- Upload input file to s3 ----

        s3 = S3Client.builder().region(region).build();             // Build S3 client
        uploadInputFile(input_file, s3, bucket, inputFileKey);      // Upload input File to S3


         // ---- Upload first message to sqs
        String QUEUE_NAME = "localManagerQ" + new Date().getTime();
        SqsClient sqs = SqsClient.builder().region(region).build(); // Build Sqs client
        createQByName(QUEUE_NAME, sqs);                             // Creat Q
        String queueUrl = getQUrl(QUEUE_NAME, sqs);
        putInputUrlInSqs(inputFileKey, bucket, sqs, queueUrl);      // Put inputFile Url in SQS

        System.out.println("success uploading to sqs");


    }

    private static void putInputUrlInSqs(String inputFileKey, String bucket, SqsClient sqs, String queueUrl) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(getFileUrl(bucket, inputFileKey))
                .delaySeconds(5)
                .build();
        sqs.sendMessage(send_msg_request);
    }

    private static String getQUrl(String QUEUE_NAME, SqsClient sqs) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(QUEUE_NAME)
                .build();
        //get url in order to send later
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
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

    private static void uploadInputFile(File input_file, S3Client s3, String bucket, String key) {

        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .build())
                .build());


        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromFile(input_file));
    }

//    s3://bucket1585474884962/inputFile
    private static String getFileUrl (String bucket, String key) {
        return "s3://" + bucket + "/" + key;
    }





}
