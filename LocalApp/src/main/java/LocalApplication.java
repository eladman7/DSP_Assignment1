import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalApplication {
    private final static Logger log = LoggerFactory.getLogger(LocalApplication.class);
    private final static String LocalManagerQName = "Local_Manager_Queue";
    private final static String LocalAppId = String.valueOf(System.currentTimeMillis());

    public static void main(String[] args) throws InterruptedException, IOException {

        String input_file_path = args[0]; // input file path
        String output_file_name = args[1]; // output file path
        int numOfPdfPerWorker = Integer.parseInt(args[2]); // n - work per worker
        boolean terminate = args.length == 4 && Boolean.parseBoolean(args[3]); // terminate?

        // ---- Upload input file to s3 ----
        String inputFileKey = LocalAppId + "/" + "inputFile" + LocalAppId;
        S3Utils.uploadFile(input_file_path, inputFileKey, S3Utils.PRIVATE_BUCKET);
        String fileUrl = S3Utils.getFileUrl(inputFileKey);
        log.info("Input file successfully uploaded here: {}", fileUrl);

        // ---- Upload first message to sqs
        sendNewTask(numOfPdfPerWorker, fileUrl);

        // ---- Create Manager Instance
        setupManager();

        // ---- Read SQS summary message from manager
        handleSummaryMsg(output_file_name, terminate);
        log.info("Done");
    }

    private static void handleSummaryMsg(String output_file_name, boolean terminate) throws InterruptedException, IOException {
        log.debug("building manager < -- > local queue");
        String managerLocalQName = "Manager_Local_Queue" + LocalAppId;
        SQSUtils.buildQueueIfNotExists(managerLocalQName);
        log.info("Waiting for a summary...");
        String summaryMessage;
        Message sMessage;
        while (true) {
            try {
                sMessage = SQSUtils.recieveMSG(managerLocalQName);
                if (sMessage != null) {
                    summaryMessage = sMessage.body();
                    if (summaryMessage != null)
                        break;
                }
            } catch (SqsException | SdkClientException sqsExecption) {
                log.error("LocalApplication.main(): got SqsException... " + sqsExecption.getMessage() +
                        "\nsleeping & retrying!");
                Thread.sleep(1000);
            }
        }
        SQSUtils.deleteMSG(sMessage, managerLocalQName);
        log.info("Got summary file. Creating output file");
        String summaryBucket = extractBucket(summaryMessage);
        String summaryKey = extractKey(summaryMessage);
        S3Utils.getObjectToLocal(summaryKey, summaryBucket, "summaryFile" + LocalAppId + ".txt");
        makeOutputFile("summaryFile" + LocalAppId + ".txt", output_file_name);
        log.debug("deleting Local app Q's");
        deleteLocalAppQueues();
        if (terminate) {
            SQSUtils.sendMSG(LocalManagerQName, "terminate");
            log.debug("Local sent terminate message and finish..deleting local Q's.. Bye");
        }
    }

    private static void sendNewTask(int numOfPdfPerWorker, String fileUrl) {
        SQSUtils.buildQueueIfNotExists(LocalManagerQName);
        SQSUtils.sendMSG(LocalManagerQName, fileUrl + " " + numOfPdfPerWorker);
        log.info("New task message successfully sent");
    }

    private static void setupManager() {
        if (!EC2Utils.isInstanceRunning("Manager")) {
            log.info("There is no running manager.. launch manager");
            String managerScript = createManagerUserData();
            EC2Utils.createEc2Instance("Manager", managerScript, 1);
            log.info("Manager launched successfully");
        } else log.info("Ec2 manager already running.. ");
    }


    private static void makeOutputFile(String fileName, String outputFileName) throws IOException {
        log.debug("Start making output file.");
        BufferedReader reader;
        String line;
        String op, inputLink, rest;
        reader = new BufferedReader(new FileReader(fileName));
        FileWriter summaryFile = new FileWriter(outputFileName + ".html");
        summaryFile.write("<!DOCTYPE html>\n<html>\n<body>\n");
        line = reader.readLine();
        while (line != null) {
            String[] resLine = line.split("\\s+");
            op = resLine[0];
            inputLink = resLine[1];
            rest = String.join(" ", Arrays.copyOfRange(resLine, 2, resLine.length));

            if (conversionSucceeded(rest, S3Utils.PRIVATE_BUCKET)) {
                summaryFile.write("<p>" + op + " " + inputLink + " " + "<a href=" + rest + ">" + rest + "</a></p>\n");
            } else {
                summaryFile.write("<p>" + op + " " + inputLink + " " + rest + "</p>\n");
            }
            line = reader.readLine();

        }
        //Add html epilogue
        summaryFile.write("</body>\n</html>");
        summaryFile.close();
        summaryFile.close();
        reader.close();
        log.debug("Finish making summary file.");
    }

    public static boolean conversionSucceeded(String link, String bucketName) {
        String prefix = "https://" + bucketName + ".s3.amazonaws.com";
        if (link.length() < prefix.length())
            return false;
        return prefix.equals(link.substring(0, prefix.length()));

    }

    private static String createManagerUserData() {
        String script = "#!/bin/bash\n"
                + "aws s3 cp " + S3Utils.getFileUrl("jars/managerapp") + " /home/ec2-user/Manager.jar\n"
                + "java -jar /home/ec2-user/Manager.jar " + "\n";
        log.debug("user data: " + script);
        return script;
    }

    private static void deleteLocalAppQueues() {
        SQSUtils.deleteQ("Manager_Local_Queue" + LocalAppId);
        SQSUtils.deleteQ("TasksResultsQ" + LocalAppId);
    }

    /**
     * @param body message body
     * @return the bucket name from a sqs message
     */
    public static String extractBucket(String body) {
        Pattern pattern = Pattern.compile("//(.*?)/((.+?)*)");
        Matcher matcher = pattern.matcher(body);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return " ";
    }

    /**
     * @param body message body
     * @return the key from a sqs message
     */
    public static String extractKey(String body) {
        Pattern pattern = Pattern.compile("//(.*?)/((.+?)*)");
        Matcher matcher = pattern.matcher(body);
        if (matcher.find()) {

            return matcher.group(2);
        }
        return " ";
    }

}
