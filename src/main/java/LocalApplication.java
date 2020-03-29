import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.util.List;

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
        String amiId =  "ami-b66ed3de";
        String ec2NameManager = "manager";



        // ---- Upload input file to s3 ----

        s3 = S3Client.builder().region(region).build();             // Build S3 client
//        uploadInputFile(input_file, s3, bucket, inputFileKey);      // Upload input File to S3


         // ---- Upload first message to sqs
/*      String QUEUE_NAME = "local <--> ManagerQ" + new Date().getTime();
        SqsClient sqs = SqsClient.builder().region(region).build(); // Build Sqs client
        createQByName(QUEUE_NAME, sqs);                             // Creat Q
        String queueUrl = getQUrl(QUEUE_NAME, sqs);
        putInputUrlInSqs(inputFileKey, bucket, sqs, queueUrl);      // Put inputFile Url in SQS

        System.out.println("success uploading to sqs");
*/
        // ---- Create Manager Instance
        Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
        if (!isManagerRunning(ec2)) {
            System.out.println("There is no manager running.. lunch manager");
            createEc2Instance(ec2, amiId, ec2NameManager);
        }
        else System.out.println("ec2 manager already running.. ");

    }





    public static boolean isManagerRunning(Ec2Client ec2) {
            String nextToken = null;
                 do {
                    DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                    DescribeInstancesResponse response = ec2.describeInstances(request);

                    for (Reservation reservation : response.reservations()) {
                        for (Instance instance : reservation.instances()) {
                            List<Tag> tagList = instance.tags();
                            for (Tag tag : tagList) {
                                System.out.println(tag.key());
                                System.out.println(tag.value());
                                if (tag.value().equals("manager"))
                                    return true;
                            }
                        }
                    }
                    nextToken = response.nextToken();
                } while (nextToken != null);

            return false;
        }


    private static void printList(CreateTagsRequest tagsRequest) {
        for (Tag tag : tagsRequest.tags()) {
            System.out.println("key: " + tag.key());
            System.out.println("val: " + tag.value());
        }
    }


    private static void createEc2Instance(Ec2Client ec2, String amiId, String ec2NameManager) {

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
                .value(ec2NameManager)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully started EC2 instance %s based on AMI %s",
                    instanceId, amiId);

        } catch (Ec2Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
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
