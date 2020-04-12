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


    @Override
    public void run() {
        String amiId = "ami-076515f20540e6e0b";
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
            int messageCount = tasks.size();
            System.out.println("numOfMessages: " + messageCount);

            // TODO: 10/04/2020 add synchronization somehow
            int numOfRunningWorkers = EC2Utils.numOfRunningWorkers();

            int numOfWorkers;
            if (numOfRunningWorkers == 0) {
                // TODO: 11/04/2020 what if messageCount is smaller than numOfMsfPerWorker?
                numOfWorkers = messageCount / numOfMsgForWorker;      // Number of workers the job require.
            } else numOfWorkers = (messageCount / numOfMsgForWorker) - numOfRunningWorkers;

            // Build Tasks Q name
            // TODO: 10/04/2020 stop this stupid check after the first time.
            SQSUtils.BuildQueueIfNotExists(tasksQName);
            System.out.println("build TasksQ succeed");

            // Build Workers output Q
            SQSUtils.BuildQueueIfNotExists(workerOutputQName);
            System.out.println("build Workers outputQ succeed");

            // Delegate Tasks to workers.
            for (String task : tasks) {
                System.out.println("task: " + task);
                SQSUtils.sendMSG(tasksQName, task + " " + id);
            }
            System.out.println("Delegated all tasks to workers, now waiting for them to finish.." +
                    "(it sounds like a good time to lunch them!)");
            //assert there are no more than 10 workers running.
            if (numOfWorkers + numOfRunningWorkers < 10) {
                if (numOfWorkers > 0)
                    createWorkers(numOfWorkers, amiId);
            } else {
                System.out.println("tried to build more than 10 instances.. exit..");
                return;
            }
            System.out.println("create workers succeed");
            System.out.println("Start making summary file.. ");
            makeAndUploadSummaryFile(messageCount);

            System.out.println("finish make and upload summary file");
            System.out.println("ManagerRunner with id: " + id + " exited!");
        }
    }


    /**
     * Make summary file from all workers results and upload to s3 bucket, named "summaryfilebucket"
     * so in order to make this work there is bucket with this name before the function run
     *
     * @param numOfMessages
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
                System.out.println("ManagerRunner with id: " + id);
                for (Message message : messages) {
                    summaryFile.write(message.body() + '\n');
                    SQSUtils.deleteMSG(message, workerOutputQName);
                    leftToRead--;
                    System.out.println();
                }
            }
            summaryFile.close();
            System.out.println("RunInstancesResponse response finish making summaryFile.. start uploading summary file..");
            S3Utils.uploadFile("summaryFile" + id + ".txt",
                    "summaryFile", S3Utils.PRIVATE_BUCKET, false);

            System.out.println("finish uploading file..put message in sqs ");
            SQSUtils.sendMSG("Manager_Local_Queue" + id, getFileUrl("dsp-private-bucket", "summaryFile"));
        } catch (Exception ex) {
            System.out.println("ManagerRunner failed to create final summary file. stop running!");
            System.out.println(ex.toString());
        }
    }

    /**
     * This function create numOfWorker Ec2-workers.
     *
     * @param numOfWorkers
     * @param amiId
     */
    public void createWorkers(int numOfWorkers, String amiId) {
        String[] instancesNames = new String[numOfWorkers];
        for (int i = 0; i < numOfWorkers; i++) {
            instancesNames[i] = "WorkerNumber" + i;
        }
        EC2Utils.createEc2Instance(amiId, instancesNames, createWorkerUserData(), numOfWorkers);
    }

    private String createWorkerUserData() {
        String bucketName = S3Utils.PRIVATE_BUCKET;
        String fileKey = "workerapp";
        String s3Path = "https://" + bucketName + ".s3.amazonaws.com/" + fileKey;
        String script = "#!/bin/bash\n"
                + "wget " + s3Path + " -O /home/ec2-user/worker.jar\n" +
                "java -jar /home/ec2-user/worker.jar " + this.tasksQName + " " + this.workerOutputQName + "\n";
        System.out.println("user data: " + script);
        return script;
    }

    /**
     * @param body
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
     * @param body
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
     * @param filename
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
                // TODO: 11/04/2020  assert valid input
//          if (!line.equals(""))
                tasks.add(line);
                line = reader.readLine();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return tasks;
    }

    private String getFileUrl(String bucket, String key) {
        return "s3://" + bucket + "/" + key;
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



