import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.utils.CollectionUtils;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Manager {
    public static void main(String[] args) {
        // Currently assuming there is only one LocalApplication.
        final String sqsName = "Local_Manager_Queue";           // Save the name of the Local <--> Manager sqs
        final String tasksQName = "TasksQueue";

        // Read message from Local
        // Busy Wait until terminate message
        List<Message> messages;
        Message inputMessage;
        ExecutorService executor = Executors.newCachedThreadPool();
        ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;

        // Build Tasks Q
        SQSUtils.BuildQueueIfNotExists(tasksQName);
        System.out.println("Manager build TasksQ - succeed");


        while (true) {
            try {
                messages = SQSUtils.recieveMessages(sqsName, 0, 1);
                if (!CollectionUtils.isNullOrEmpty(messages)) {
                    inputMessage = messages.get(0);
                    // Terminate stay as is, only one send terminate and we done with this.
                    if (inputMessage.body().equals("terminate") && messages.size() == 1) {
                        System.out.println("manager get terminate message, deleting terminate message");
                        SQSUtils.deleteMSG(inputMessage, sqsName);
                        System.out.println("waiting for all local apps connections to finish");
                        executor.shutdown();
                        waitExecutorToFinish(executor);
                        System.out.println("terminating ec2 instances. ");
                        EC2Utils.terminateEc2Instances();
                        System.out.println("succeed terminate all ec2 instances, start deleting TasksQueue process");
                        SQSUtils.deleteQ("TasksQueue");
                        System.out.println("Deleting Local < -- > Manager Queue..");
                        SQSUtils.deleteQ("Local_Manager_Queue");
                        break;
                    }
                    else if (isS3Message(inputMessage.body())) {
                        int numOfMsgForWorker = extractN(inputMessage);
                        System.out.println("Manager executing runner with ResultQ: TasksResultsQ" + extractId(inputMessage.body())
                                + " msgPerWorker: " + numOfMsgForWorker);
                        pool.execute(new ManagerRunner("TasksQueue",
                                "TasksResultsQ", numOfMsgForWorker, inputMessage.body(), extractId(inputMessage.body())));
                        System.out.println("Pool active thread count: " + pool.getActiveCount());
                        SQSUtils.deleteMSG(inputMessage, sqsName);
                    }
                }
            } catch (IndexOutOfBoundsException exception) {
                System.out.println(exception.getMessage());
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
            System.out.println(exception.getMessage());
        }
    }

    private static boolean isS3Message(String inputMessage) {
        return "s3://".equals(inputMessage.substring(0, 5));

    }

    /**
     *
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


