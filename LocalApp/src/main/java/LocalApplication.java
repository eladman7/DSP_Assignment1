import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalApplication {

    public static void main(String[] args) {
        final String localAppId = String.valueOf(System.currentTimeMillis());
        //The name of the input file in resources folder.
        String input_file_name = args[0];
        String output_file_name = args[1];
        int numOfPdfPerWorker = Integer.parseInt(args[2]);
        boolean terminate = args.length > 3 && "terminate".equals(args[3]);

        String inputFileKey = "inputFile" + localAppId;

        // ---- Upload input file to s3 ----

        S3Utils.uploadFile(input_file_name, inputFileKey, S3Utils.PRIVATE_BUCKET, true);      // Upload input File to S3
        System.out.println("success upload input file");

        // ---- Upload first message to sqs
        String LocalManagerQName = "Local_Manager_Queue";
        String fileUrl = getFileUrl(inputFileKey);
        System.out.println("file is here: " + fileUrl);
        SQSUtils.sendMSG(LocalManagerQName, fileUrl + " " + numOfPdfPerWorker);
        System.out.println("success uploading first message to sqs");

        // ---- Create Manager Instance
        if (!EC2Utils.isManagerRunning()) {
            System.out.println("There is no running manager.. lunch manager");
            // Run manager JarFile with input : numOfPdfPerWorker.
            String managerScript = createManagerUserData();
            EC2Utils.createEc2Instance("Manager", managerScript, 1);
            System.out.println("Success lunching manager");
        } else System.out.println("Ec2 manager already running.. ");

        // ---- Read SQS summary message from manager
        System.out.println("building manager < -- > local queue");
        String ManagerLocalQName = "Manager_Local_Queue" + localAppId;
        SQSUtils.BuildQueueIfNotExists(ManagerLocalQName);

        // receive messages from the queue, if empty? (maybe busy wait?)
        System.out.println("waiting for a summary file from manager..");
        String summaryMessage;
        Message sMessage;
        //busy wait..
        while (true) {
            sMessage = SQSUtils.recieveMSG(ManagerLocalQName);
            if (sMessage != null) {
                summaryMessage = sMessage.body();
                if (summaryMessage != null)
                    break;
            }
        }
        SQSUtils.deleteMSG(sMessage, ManagerLocalQName);
        System.out.println("local app gets its summary file.. download and sent termination message if needed");

        //Download summary file and create Html output
        String summaryBucket = extractBucket(summaryMessage);
        String summaryKey = extractKey(summaryMessage);
        try {
            S3Utils.getObjectToLocal(summaryKey, summaryBucket, "summaryFile" + localAppId + ".txt");

            makeSummaryFile("summaryFile" + localAppId + ".txt", output_file_name);
        } catch (Exception getObjException) {
            System.out.println(getObjException.getMessage());
            //send termination message if needed
        } finally {
            //We want to delete this special local app Q any way when finish.
            System.out.println("deleting LA Q's");
            deleteLocalAppQueues(localAppId);
            if (terminate) {
                SQSUtils.sendMSG(LocalManagerQName, "terminate");
                System.out.println("Local sent terminate message and finish..deleting local Q's.. Bye");
            }
        }
    }


    private static void makeSummaryFile(String fileName, String outputFileName) throws IOException {
        System.out.println("Start making summary file.");
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

            if (conversionSucceeded(rest, S3Utils.PUBLIC_BUCKET)) {
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
        System.out.println("Finish making summary file.");

    }

    public static boolean conversionSucceeded(String link, String bucketName) {
        String prefix = "https://" + bucketName + ".s3.amazonaws.com";
        if (link.length() < prefix.length())
            return false;
        return prefix.equals(link.substring(0, prefix.length()));

    }

    private static String createManagerUserData() {
        String fileKey = "managerapp";
        System.out.println("Uploading manager jar..");
        S3Utils.uploadFile("/home/bar/IdeaProjects/Assignment1/out/artifacts/Manager_jar/Manager.jar",
                fileKey, S3Utils.PRIVATE_BUCKET, false);

        System.out.println("Uploading worker jar..");
        S3Utils.uploadFile("/home/bar/IdeaProjects/Assignment1/out/artifacts/Worker_jar/Worker.jar",
                "workerapp", S3Utils.PRIVATE_BUCKET, false);
        System.out.println("Finish upload jars.");

        String s3Path = "https://" + S3Utils.PRIVATE_BUCKET + ".s3.amazonaws.com/" + fileKey;
        String script = "#!/bin/bash\n"
                + "wget " + s3Path + " -O /home/ec2-user/Manager.jar\n" +
                "java -jar /home/ec2-user/Manager.jar " + "\n";
        System.out.println("user data: " + script);
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
