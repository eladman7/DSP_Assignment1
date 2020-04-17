import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Manager {
    private final static Logger log = LoggerFactory.getLogger(Manager.class);

    public static void main(String[] args) throws InterruptedException {
        log.info("Manager is up...");
        final String sqsName = "Local_Manager_Queue";           // Save the name of the Local <--> Manager sqs
        final String tasksQName = "TasksQueue";

        // Read message from Local
        // Busy Wait until terminate message
        Message inputMessage;
        ExecutorService executor = Executors.newCachedThreadPool();
        ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;

        // Build Tasks Q
        Map<QueueAttributeName, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.VISIBILITY_TIMEOUT, "60");
        SQSUtils.buildQueueIfNotExists(tasksQName, attributes);
        log.debug("Manager build TasksQ - succeed");

        while (true) {
            try {
                inputMessage = SQSUtils.recieveMSG(sqsName);
                if (inputMessage != null) {
                    // Terminate stay as is, only one send terminate and we done with this.
                    if (inputMessage.body().equals("terminate")) {
                        log.info("manager get terminate message, deleting terminate message");
                        SQSUtils.deleteMSG(inputMessage, sqsName);
                        log.info("waiting for all local apps connections to finish");
                        executor.shutdown();
                        waitExecutorToFinish(executor);
                        log.info("terminating ec2 instances. ");
                        EC2Utils.terminateEc2Instances();
                        log.info("succeed terminate all ec2 instances, start deleting TasksQueue process");
                        SQSUtils.deleteQ("TasksQueue");
                        log.info("Deleting Local < -- > Manager Queue..");
                        SQSUtils.deleteQ("Local_Manager_Queue");
                        break;
                    } else if (isS3Message(inputMessage.body())) {
                        int numOfMsgForWorker = extractN(inputMessage);
                        log.info("Manager executing runner with ResultQ: TasksResultsQ" + extractId(inputMessage.body())
                                + " msgPerWorker: " + numOfMsgForWorker);
                        pool.execute(new ManagerRunner("TasksQueue",
                                "TasksResultsQ", numOfMsgForWorker, inputMessage.body(), extractId(inputMessage.body())));
                        SQSUtils.deleteMSG(inputMessage, sqsName);
                    }
                }
            } catch (SqsException sqsExecption) {
                log.error("Manager.main(): got SqsException... {} sleeping & retrying!", sqsExecption.getMessage());
                Thread.sleep(1000);
            }
        }

    }

    private static int extractN(Message msg) {
        return Integer.parseInt(msg.body().split("\\s+")[1]);
    }

    /**
     * Waiting for some LocalApp < --- > Manager connection to finish.
     *
     * @param executor pool service
     */
    private static void waitExecutorToFinish(ExecutorService executor) {
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        } catch (InterruptedException exception) {
            log.error(exception.getMessage());
        }
    }

    private static boolean isS3Message(String inputMessage) {
        return "s3://".equals(inputMessage.substring(0, 5));

    }

    /**
     * @return the key from a sqs message
     */
    public static String extractId(String messagePath) {
        Pattern pattern = Pattern.compile("(.*?)inputFile((.+?)*)");
        Matcher matcher = pattern.matcher(messagePath);
        if (matcher.find()) {

            return matcher.group(2).split("\\s+")[0];
        }
        return " ";
    }
}


