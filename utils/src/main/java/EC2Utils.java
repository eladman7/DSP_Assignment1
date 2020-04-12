import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.Base64;
import java.util.List;

public class EC2Utils {
    private final static Ec2Client ec2 = Ec2Client.builder()
            .region(Region.US_EAST_1)
            .build();
    private final static String amiId = "ami-076515f20540e6e0b";


    public static void createEc2Instance(String ec2Name, String userDataScript, int instancesCount) {
        String[] name = {ec2Name};
        createEc2Instance(name, userDataScript, instancesCount);
    }

    /**
     * create an Ec2 instance
     *
     * @param ec2Name name for the instance
     */
    public static void createEc2Instance(String[] ec2Name, String userDataScript, int instancesCount) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .arn("arn:aws:iam::110380217222:instance-profile/assignment1")
                        .build())
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(instancesCount)
                .minCount(instancesCount)
                .userData(Base64.getEncoder().encodeToString(userDataScript.getBytes()))
                .keyName("key1")
                .build();
        RunInstancesResponse response = ec2.runInstances(runRequest);
        for (int i = 0; i < instancesCount; i++) {
            String instanceId = response.instances().get(i).instanceId();
            Tag tag = Tag.builder()
                    .key("name")
                    .value(ec2Name[i])
                    .build();
            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();
            try {
                ec2.createTags(tagRequest);
                System.out.println(
                        "Successfully started EC2 instance: " + instanceId + "based on AMI: " + amiId);

            } catch (Ec2Exception e) {
                System.err.println("error while tagging an instance.. trying again");
                i--;
            }
        }
    }


    /**
     * @return true iff the manager running
     */
    public static boolean isManagerRunning() {
        String nextToken = null;
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    List<Tag> tagList = instance.tags();
                    for (Tag tag : tagList) {
                        if (tag.value().equals("Manager") &&
                                (instance.state().name().toString().equals("running") ||
                                        instance.state().name().toString().equals("pending")))
                            return true;
                    }
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);

        return false;
    }

    /**
     * Terminate all running ec2 instances
     */
    public static void terminateEc2Instances() {
        String nextToken = null;
        do {
            DescribeInstancesRequest dRequest = DescribeInstancesRequest.builder().nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(dRequest);
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    TerminateInstancesRequest request = TerminateInstancesRequest.builder().instanceIds(instance.instanceId()).build();
                    ec2.terminateInstances(request);
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);
    }


    //Local App should wait for other before lunching Workers.
    public synchronized static void lunchWorkers(int messageCount, int numOfMsgForWorker, String tasksQName, String workerOutputQName) {
        int numOfRunningWorkers = EC2Utils.numOfRunningWorkers();
        int numOfWorkers;
        if (numOfRunningWorkers == 0) {
            // TODO: 11/04/2020 what if messageCount is smaller than numOfMsfPerWorker?
            numOfWorkers = messageCount / numOfMsgForWorker;      // Number of workers the job require.
        } else numOfWorkers = (messageCount / numOfMsgForWorker) - numOfRunningWorkers;

        //assert there are no more than 10 workers running.
        if (numOfWorkers + numOfRunningWorkers < 10) {
            if (numOfWorkers > 0)
                createWorkers(numOfWorkers, tasksQName, workerOutputQName);
        } else {

            System.out.println("tried to build more than 10 instances ...");
            if (numOfRunningWorkers == 0)
                //This is the case where numOfRunning=0 & numOfWorkers>=10. 9 + manager = 10;
                createWorkers(9, tasksQName, workerOutputQName);
        }
    }

    /**
     * This function create numOfWorker Ec2-workers.
     *
     * @param numOfWorkers how much workers to create
     */
    public static void createWorkers(int numOfWorkers, String tasksQName, String workerOutputQName) {
        String[] instancesNames = new String[numOfWorkers];
        for (int i = 0; i < numOfWorkers; i++) {
            instancesNames[i] = "WorkerNumber" + i;
        }
        EC2Utils.createEc2Instance(instancesNames, createWorkerUserData(tasksQName, workerOutputQName), numOfWorkers);
    }

    private static String createWorkerUserData(String tasksQName, String workerOutputQName) {
        String bucketName = S3Utils.PRIVATE_BUCKET;
        String fileKey = "workerapp";
        String s3Path = "https://" + bucketName + ".s3.amazonaws.com/" + fileKey;
        String script = "#!/bin/bash\n"
                + "wget " + s3Path + " -O /home/ec2-user/worker.jar\n" +
                "java -jar /home/ec2-user/worker.jar " + tasksQName + " " + workerOutputQName + "\n";
        System.out.println("user data: " + script);
        return script;
    }


    /**
     * @return the number of currently running client
     */

    public static int numOfRunningWorkers() {
        String nextToken = null;
        int counter = 0;
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    List<Tag> tagList = instance.tags();
                    for (Tag tag : tagList) {
                        if (!tag.value().equals("Manager") &&
                                (instance.state().name().toString().equals("running") ||
                                        instance.state().name().toString().equals("pending")))
                            counter++;
                    }
                }
            }
            nextToken = response.nextToken();
        } while (nextToken != null);

        return counter;
    }

}
