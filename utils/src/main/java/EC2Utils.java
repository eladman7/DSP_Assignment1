import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.Base64;
import java.util.List;

public class EC2Utils {
    private final static Ec2Client ec2 = Ec2Client.builder()
            .region(Region.US_EAST_1)
            .build();

    public static void createEc2Instance(String amiId, String ec2Name, String userDataScript, int instancesCount) {
        String[] name = {ec2Name};
        createEc2Instance(amiId, name, userDataScript, instancesCount);
    }

    /**
     * create an Ec2 instance
     *
     * @param amiId
     * @param ec2Name
     */
    public static void createEc2Instance(String amiId, String[] ec2Name, String userDataScript, int instancesCount) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .arn("arn:aws:iam::882762034269:instance-profile/role1")
                        .build())
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(instancesCount)
                .minCount(instancesCount)
                .userData(Base64.getEncoder().encodeToString(userDataScript.getBytes()))
                .keyName("eladkey")
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
                        if (tag.value().equals("manager") &&
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
