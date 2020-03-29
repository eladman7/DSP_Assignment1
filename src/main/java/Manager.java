import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Manager {

    public static void main(String[] args) {
        S3Client s3;
        Region region = Region.US_EAST_1;
        String amiId = "ami-b66ed3de";
        String ec2NameManager = "manager";

        String inputFileName = args[0];     // Save the input file name, get it from LocalApp
        String sqsName = args[1];           // Save the name of the Local <--> Manager sqs
        int numOfMsgForWorker = Integer.parseInt(args[2]);  // Save number of msgs for each worker
        s3 = S3Client.builder().region(region).build();
        SqsClient sqs = SqsClient.builder().region(region).build(); // Build Sqs client
        String qUrl = getQUrl(sqsName, sqs);


        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(qUrl)   //which queue
                .build();
        List<Message> messages;
        String inputMessage;

        // TODO: 29/03/2020 change to something prettier
        //busy wait..
        while (true) {
            try {
                messages = sqs.receiveMessage(receiveRequest).messages();
                inputMessage = messages.get(0).body();
                break;
            } catch (IndexOutOfBoundsException ignored) {}
        }

        if (inputMessage.equals("terminate")) {
            System.out.println("terminate.. ");
        } else {

            String inputBucket = extractBucket(inputMessage);
            String inputKey = extractKey(inputMessage);
            s3.getObject(GetObjectRequest.builder().bucket(inputBucket).key(inputKey).build(), //download input file
                    ResponseTransformer.toFile(Paths.get("inputFile.txt")));
            // TODO: 29/03/2020 continue from the 4th square at the assignment page -  "The Manager"
            List<String> tasks = createSqsMessages("inputFile.txt");


        }

    }

    // TODO: 29/03/2020 this function exists in LocalApp too.
    private static String getQUrl(String QUEUE_NAME, SqsClient sqs) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(QUEUE_NAME)
                .build();
        //get url in order to send later
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }

    public static String extractBucket(String body) {
        Pattern pattern = Pattern.compile("//(.*?)/((.+?)*)");
        Matcher matcher = pattern.matcher(body);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return " ";
    }

    public static String extractKey(String body) {
        Pattern pattern = Pattern.compile("//(.*?)/((.+?)*)");
        Matcher matcher = pattern.matcher(body);
        if (matcher.find()) {

            return matcher.group(2);
        }
        return " ";
    }

    public static List<String> createSqsMessages(String filename) {
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

}
