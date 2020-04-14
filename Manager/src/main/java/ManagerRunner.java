import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ManagerRunner implements Runnable {
    private final String tasksQName;
    private final int numOfMsgForWorker;
    private final String inputMessage;
    private final String workerOutputQName;
    private final String id;

    public ManagerRunner(String tasksQName, String workerOutputQ, int numOfMsgForWorker, String inputMessage, String id) {
        this.id = id;
        this.tasksQName = tasksQName;
        this.numOfMsgForWorker = numOfMsgForWorker;
        this.inputMessage = inputMessage;
        this.workerOutputQName = workerOutputQ + id;

    }
// TODO: 13/04/2020 Add delete files.

    @Override
    public void run() {
        String inputBucket = extractBucket(inputMessage);
        String inputKey = extractKey(inputMessage);
        //download input file, Save under "inputFile.txt"
        try {
            S3Utils.getObjectToLocal(inputKey, inputBucket, "inputFile" + id + ".txt");
        } catch (Exception getObj) {
            System.out.println(getObj.getMessage());
        } finally {

            // Create SQS message for each url in the input file.
            List<String> tasks = createSqsMessages("inputFile" + id + ".txt");

            // Build Workers output Q
            SQSUtils.BuildQueueIfNotExists(workerOutputQName);
            System.out.println("build Workers outputQ succeed");


            int messageCount = tasks.size();
            System.out.println("numOfMessages: " + messageCount);

            // Delegate Tasks to workers.
            for (String task : tasks) {
                System.out.println("task: " + task);
                SQSUtils.sendMSG(tasksQName, task + " " + id);
            }
            System.out.println("Delegated all tasks to workers, now waiting for them to finish..");

            System.out.println("Lunching Workers..");
            EC2Utils.launchWorkers(messageCount, numOfMsgForWorker, this.tasksQName, "TasksResultsQ");
            System.out.println("Finished lunching workers process.");

            System.out.println("Start making summary file.. ");
            makeAndUploadSummaryFile(messageCount);

            System.out.println("finish make and upload summary file");
            System.out.println("deleting file.. ");

            System.out.println("ManagerRunner with id: " + id + " exited!");
        }
    }


    /**
     * Make summary file from all workers results and upload to s3 bucket, named "summaryfilebucket"
     * so in order to make this work there is bucket with this name before the function run
     *
     * @param numOfMessages number of messages we got
     */
    private void makeAndUploadSummaryFile(int numOfMessages) {
        int leftToRead = numOfMessages;
        FileWriter summaryFile;
        try {
            summaryFile = new FileWriter("summaryFile" + id + ".txt");
            System.out.println("ManagerRunner with id: " + id + " expecting to read: " + numOfMessages + " msgs"
                    + " from Q: " + workerOutputQName);
            while (leftToRead > 0) {
                List<Message> messages = SQSUtils.recieveMessages(workerOutputQName, 0, 1);
                for (Message message : messages) {
                    summaryFile.write(message.body() + '\n');
                    SQSUtils.deleteMSG(message, workerOutputQName);
                    leftToRead--;
                }
            }
            summaryFile.close();
            System.out.println("RunInstancesResponse response finish making summaryFile.. start uploading summary file..");
            S3Utils.uploadFile("summaryFile" + id + ".txt",
                    "summaryFile", S3Utils.PRIVATE_BUCKET, false);

            System.out.println("finish uploading file..put message in sqs ");
            SQSUtils.sendMSG("Manager_Local_Queue" + id, getFileUrl("summaryFile"));

        } catch (Exception ex) {
            System.out.println("ManagerRunner failed to create final summary file. stop running!");
            System.out.println(ex.toString());
        }
    }


    /**
     * @param body message body
     * @return the bucket name from a sqs message
     */
    public String extractBucket(String body) {
        Pattern pattern = Pattern.compile("//(.*?)/((.+?)*)");
        Matcher matcher = pattern.matcher(body);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return " ";
    }

    /**
     * @return the key from a sqs message
     */
    public String extractKey(String body) {
        Pattern pattern = Pattern.compile("//(.*?)/((.+?)*)");
        Matcher matcher = pattern.matcher(body);
        if (matcher.find()) {

            return matcher.group(2).split("\\s+")[0];
        }
        return " ";

    }

    /**
     * @param filename file name
     * @return List of all the messages from the pdf file, which we get by sqs.
     */
    public List<String> createSqsMessages(String filename) {
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

    private String getFileUrl(String key) {
        return "s3://" + S3Utils.PRIVATE_BUCKET + "/" + key;
    }

    @Override
    public String toString() {
        return "ManagerRunner{" +
                "TasksQName='" + tasksQName + '\'' +
                ", numOfMsgForWorker=" + numOfMsgForWorker +
                ", inputMessage='" + inputMessage + '\'' +
                ", workerOutputQName='" + workerOutputQName + '\'' +
                ", id='" + id + '\'' +
                '}';
    }

}



