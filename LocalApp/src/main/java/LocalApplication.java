import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalApplication {
    private final static Logger log = LoggerFactory.getLogger(LocalApplication.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        final String localAppId = String.valueOf(System.currentTimeMillis());
        String input_file_path = args[0]; // input file path
        String output_file_name = args[1]; // output file path
        int numOfPdfPerWorker = Integer.parseInt(args[2]); // n - work per worker
        boolean terminate = args.length == 4 && Boolean.parseBoolean(args[3]); // terminate?
        String inputFileKey = localAppId + "/" + "inputFile" + localAppId;

        // ---- Upload input file to s3 ----
        S3Utils.uploadFile(input_file_path, inputFileKey, S3Utils.PRIVATE_BUCKET);      // Upload input File to S3
        String fileUrl = getFileUrl(inputFileKey);
        log.info("Input file successfully uploaded here: {}", fileUrl);

        // ---- Upload first message to sqs
        String LocalManagerQName = "Local_Manager_Queue";
        Map<QueueAttributeName, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.VISIBILITY_TIMEOUT, "120");
        SQSUtils.buildQueueIfNotExists(LocalManagerQName, attributes);
        SQSUtils.sendMSG(LocalManagerQName, fileUrl + " " + numOfPdfPerWorker);
        log.info("New task message successfully sent");

        // ---- Create Manager Instance
        if (!EC2Utils.isInstanceRunning("Manager")) {
            log.debug("There is no running manager.. launch manager");
//            uploadJars();
            String managerScript = createManagerUserData();
            EC2Utils.createEc2Instance("Manager", managerScript, 1);
            log.info("Manager launched successfully");
        } else log.debug("Ec2 manager already running.. ");

        // ---- Read SQS summary message from manager
        log.debug("building manager < -- > local queue");
        String ManagerLocalQName = "Manager_Local_Queue" + localAppId;
        SQSUtils.buildQueueIfNotExists(ManagerLocalQName);
        log.info("Waiting for a summary...");
        String summaryMessage;
        Message sMessage;
        while (true) {
            try {
                sMessage = SQSUtils.recieveMSG(ManagerLocalQName);
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
        SQSUtils.deleteMSG(sMessage, ManagerLocalQName);
        log.info("Got summary file. Creating output file");
        //Download summary file and create Html output
        String summaryBucket = extractBucket(summaryMessage);
        String summaryKey = extractKey(summaryMessage);
        S3Utils.getObjectToLocal(summaryKey, summaryBucket, "summaryFile" + localAppId + ".txt");
        makeOutputFile("summaryFile" + localAppId + ".txt", output_file_name);
        //We want to delete this special local app Q any way when finish.
        log.debug("deleting LA Q's");
        deleteLocalAppQueues(localAppId);
        if (terminate) {
            SQSUtils.sendMSG(LocalManagerQName, "terminate");
            log.debug("Local sent terminate message and finish..deleting local Q's.. Bye");
        }
        log.info("Done");
    }

    private static void uploadJars() throws IOException {
        log.info("Uploading manager jar..");
        File tempFile = File.createTempFile("manager_temp", "jar");
        tempFile.deleteOnExit();
        try (InputStream in = LocalApplication.class.getClassLoader().getResourceAsStream("Manager.jar")) {
            Files.copy(in, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        S3Utils.uploadFile(tempFile, S3Utils.PRIVATE_BUCKET, "jars/managerapp");

        log.info("Uploading worker jar..");
        File tempFile2 = File.createTempFile("worker_temp", "jar");
        tempFile2.deleteOnExit();
        try (InputStream in2 = LocalApplication.class.getClassLoader().getResourceAsStream("Worker.jar")) {
            Files.copy(in2, tempFile2.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        S3Utils.uploadFile(tempFile2, S3Utils.PRIVATE_BUCKET, "jars/workerapp");
        log.info("Finish upload jars.");
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

    private static void deleteLocalAppQueues(String localAppId) {
        SQSUtils.deleteQ("Manager_Local_Queue" + localAppId);
        SQSUtils.deleteQ("TasksResultsQ" + localAppId);
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

    /**
     * Extract the file url from some s3 path
     *
     * @param key of the bucket
     * @return file url
     */
    private static String getFileUrl(String key) {
        return "s3://" + S3Utils.PRIVATE_BUCKET + "/" + key;
    }

}
