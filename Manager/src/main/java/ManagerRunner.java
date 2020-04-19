import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ManagerRunner implements Runnable {
    private final static Logger log = LoggerFactory.getLogger(Manager.class);

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
        String inputBucket = extractBucket(inputMessage);
        String inputKey = extractKey(inputMessage);
        //download input file, Save under "inputFile.txt"
        S3Utils.getObjectToLocal(inputKey, inputBucket, "inputFile" + id + ".txt");
        // Create SQS message for each url in the input file.
        List<String> tasks = createWorkerTasks("inputFile" + id + ".txt");
        // Build Workers output Q
        SQSUtils.buildQueueIfNotExists(workerOutputQName);
        log.debug("build Workers outputQ succeed");
        int messageCount = tasks.size();
        log.debug("numOfMessages: " + messageCount);
        // Delegate Tasks to workers.
        for (String task : tasks) {
            log.debug("task: " + task);
            SQSUtils.sendMSG(tasksQName, task + " " + id);
        }
        log.debug("Delegated all tasks to workers, now waiting for them to finish..");
        log.info("Lunching Workers..");
        launchWorkers(messageCount, numOfMsgForWorker, this.tasksQName, "TasksResultsQ");
        log.info("Finished lunching workers process.");
        log.info("Start making summary file.. ");
        makeAndUploadSummaryFile(messageCount);
        log.info("finish make and upload summary file");
        log.info("ManagerRunner with id: " + id + " exited!");
    }


    //Local App should wait for other before lunching Workers.
    public synchronized static void launchWorkers(int messageCount, int numOfMsgForWorker, String tasksQName, String workerOutputQName) {
        try {
            if (messageCount == 0) {
                log.error("got 0 message count! should never happen!");
                return;
            }
            int numOfRunningWorkers = EC2Utils.numOfRunningWorkers();
            // numOfNewWorkers = // Number of new workers the job require.
            int numOfNewWorkers = (messageCount <= numOfMsgForWorker) ? 1 : messageCount / numOfMsgForWorker;
            if (numOfRunningWorkers > 0) {
                numOfNewWorkers = (numOfNewWorkers <= numOfRunningWorkers) ? 0 :
                        numOfNewWorkers - numOfRunningWorkers;
                log.info("Number of running workers: " + numOfRunningWorkers + " requested workers: " +
                        numOfNewWorkers + ". No new workers will be launched!");
            }
            if (numOfNewWorkers == 0) return;
            //assert there are no more than 10 workers running.
            if (numOfNewWorkers + numOfRunningWorkers <= 9) {
                log.info("ManagerRunner launching " + numOfNewWorkers + " workers");
                bootstrapWorkers(numOfNewWorkers, tasksQName, workerOutputQName);
            } else if (numOfRunningWorkers < 9) {
                log.info("ManagerRunner launching only " + (9 - numOfRunningWorkers) + " workers. max of 9 workers reached!");
                bootstrapWorkers(9 - numOfRunningWorkers, tasksQName, workerOutputQName);
            }
        } catch (Ec2Exception ec2Ex) {
            log.error("ManagerRunner.launchWorkers(): got Ec2Exception... " + ec2Ex.getMessage());
        }
    }


    /**
     * This function create numOfWorker Ec2-workers.
     *
     * @param numOfInstances how much workers to create
     */
    public static void bootstrapWorkers(int numOfInstances, String tasksQName, String workerOutputQName) {
        String[] instancesNames = new String[numOfInstances];
        for (int i = 0; i < numOfInstances; i++) {
            instancesNames[i] = "WorkerNumber" + i;
        }
        EC2Utils.createEc2Instance(instancesNames, createWorkerUserData(tasksQName, workerOutputQName), numOfInstances);
    }

    private static String createWorkerUserData(String tasksQName, String workerOutputQName) {
        String fileKey = "jars/workerapp";
        String script = "#!/bin/bash\n"
                + "aws s3 cp " + S3Utils.getFileUrl(fileKey) + " /home/ec2-user/worker.jar\n"
                + "java -jar /home/ec2-user/worker.jar " + tasksQName + " " + workerOutputQName + "\n";
        log.debug("user data: " + script);
        return script;
    }


    /**
     * Make summary file from all workers results and upload to s3 bucket, named "summaryfilebucket"
     * so in order to make this work there is bucket with this name before the function run
     *
     * @param numOfMessages number of messages we got
     */
    private synchronized void makeAndUploadSummaryFile(int numOfMessages) {
        int leftToRead = numOfMessages;
        FileWriter summaryFile;
        String fileLocalPath = "summaryFile" + id + ".txt";
        try {
            summaryFile = new FileWriter(fileLocalPath);
            log.info("ManagerRunner with id: " + id + " expecting to read: " + numOfMessages + " msgs"
                    + " from Q: " + workerOutputQName);
            while (leftToRead > 0) {
                try {
                    Message message = SQSUtils.recieveMSG(workerOutputQName);
                    if (message != null) {
                        summaryFile.write(message.body() + '\n');
                        SQSUtils.deleteMSG(message, workerOutputQName);
                        leftToRead--;
                    }
                } catch (SqsException | SdkClientException sqsEx) {
                    log.error("ManagerRunner.makeAndUploadSummaryFile(): got SqsException "
                            + sqsEx.getMessage() + "\nsleeping & retrying");
                    Thread.sleep(1000);
                }
            }
            summaryFile.close();
            log.debug("RunInstancesResponse response finish making summaryFile.. start uploading summary file..");
            String summaryFileKey = this.id + "/" + "summaryFile";
            S3Utils.uploadFile(fileLocalPath,
                    summaryFileKey, S3Utils.PRIVATE_BUCKET);

            log.debug("finish uploading file..put message in sqs ");
            SQSUtils.sendMSG("Manager_Local_Queue" + id, getFileUrl(summaryFileKey));

        } catch (Exception ex) {
            log.error("ManagerRunner failed to create final summary file. stop running! {}", ex.getMessage());
        }
        //delete file
        File file = new File(fileLocalPath);
        if (file.delete()) {
            log.debug("File deleted successfully");
        } else {
            log.debug("Failed to delete the file");
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
    public List<String> createWorkerTasks(String filename) {
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



