import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocalApplication {

    private static final String PRIVATE_BUCKET = "dsp-private-bucket";

    public static void main(String[] args) {
        final String localAppId = String.valueOf(System.currentTimeMillis());
        String input_file_path = args[0];
        int numOfPdfPerWorker = Integer.parseInt(args[1]);
        boolean terminate = Boolean.valueOf(args[2]);
        String inputFileKey = "inputFile" + localAppId;
        String amiId = "ami-076515f20540e6e0b";

        // ---- Upload input file to s3 ----
        S3Utils.uploadFile(input_file_path, inputFileKey, PRIVATE_BUCKET, false);      // Upload input File to S3
        System.out.println("success upload input file");

        // ---- Upload first message to sqs
        String LocalManagerQName = "Local_Manager_Queue";
        String fileUrl = getFileUrl(PRIVATE_BUCKET, inputFileKey);
        SQSUtils.sendMSG(LocalManagerQName, fileUrl + " " +numOfPdfPerWorker);
        System.out.println("success uploading first message to sqs");

        // ---- Create Manager Instance
        if (!EC2Utils.isManagerRunning()) {
            System.out.println("There is no running manager.. lunch manager");
            // Run manager JarFile with input : numOfPdfPerWorker.
            String managerScript = createManagerUserData(numOfPdfPerWorker);
            EC2Utils.createEc2Instance(amiId, "Manager", managerScript, 1);
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
            S3Utils.getObjectToLocal(summaryKey, summaryBucket, "summaryFile" + localAppId + ".html");
        } catch (Exception ignored) {
            //send termination message if needed
        } finally {
            if (terminate) {
                SQSUtils.sendMSG(LocalManagerQName, "terminate");
                System.out.println("Local sent terminate message and finish..deleting local Q's.. Bye");
//                deleteLocalAppQueues(localAppId, sqs);
            } else {

            }
//            sendTerminationMessageIfNeeded(terMessage, sqs, localManagerQUrl);
        }
//                        deleteLocalAppQueues(localAppId, sqs);
    }


    // TODO: 07/04/2020 connect num of msg per worker to here
    private static String createManagerUserData(int numOfPdfPerWorker) {
        String bucketName = PRIVATE_BUCKET;
        String fileKey = "managerapp";
        System.out.println("Uploading manager jar..");
        S3Utils.uploadFile("/home/bar/IdeaProjects/Assignment1/out/artifacts/Manager_jar/Manager.jar",
                fileKey, bucketName, false);

        try {
            System.out.println("Uploading worker jar..");
            S3Utils.uploadFile("/home/bar/IdeaProjects/Assignment1/out/artifacts/Worker_jar/Worker.jar",
                    "workerapp", bucketName, false);
        } catch (Exception ex) {
            System.out.println(
                    "in LocalApplication.createManagerUserData: " + ex.getMessage());
        }

        String s3Path = "https://" + bucketName + ".s3.amazonaws.com/" + fileKey;
        String script = "#!/bin/bash\n"
                + "wget " + s3Path + " -O /home/ec2-user/Manager.jar\n" +
                "java -jar /home/ec2-user/Manager.jar " + numOfPdfPerWorker + "\n";
        System.out.println("user data: " + script);
        return script;
//        return "";
    }

    private static void deleteLocalAppQueues(String localAppId, SqsClient sqs) {
        SQSUtils.deleteQ("Manager_Local_Queue" + localAppId);
        SQSUtils.deleteQ("TasksQueue");
        SQSUtils.deleteQ("TasksResultsQ" + localAppId);
    }

    /**
     * @param body
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
     * @param body
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
     * @param bucket
     * @param key
     * @return
     */
    private static String getFileUrl(String bucket, String key) {
        return "s3://" + bucket + "/" + key;
    }

}
