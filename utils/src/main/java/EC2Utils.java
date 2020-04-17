import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class EC2Utils {
    private final static Logger log = LoggerFactory.getLogger(EC2Utils.class);

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
                        .arn("arn:aws:iam::882762034269:instance-profile/role1")
                        .build())
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(instancesCount)
                .minCount(instancesCount)
                .userData(Base64.getEncoder().encodeToString(userDataScript.getBytes()))
                .keyName("dsp_key")
                .securityGroups("default")
                .build();
        RunInstancesResponse response = ec2.runInstances(runRequest);
        int numOfRetries = 0;
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
                log.info(
                        "Successfully started EC2 instance: " + instanceId + "based on AMI: " + amiId);

            } catch (Ec2Exception e) {
                System.err.println("error while tagging an instance.. trying again");
                i--;
                numOfRetries++;
                if (numOfRetries == 100) {
                    log.warn("EC2Utils.createEc2Instance() " +
                            "stop retrying tags creation for ec2 instances after 100 attempts");
                    break;
                }
            }
        }
    }


    /**
     * @return true iff the manager running
     */
    public static boolean isInstanceRunning(String instanceName) {
        String nextToken = null;
        do {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().nextToken(nextToken).build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    List<Tag> tagList = instance.tags();
                    for (Tag tag : tagList) {
                        if (tag.value().equals(instanceName) &&
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
            List<String> workerInstanceIds = new ArrayList<>();
            for (Reservation reservation : response.reservations()) {
                workerInstanceIds.addAll(reservation.instances().stream()
                        .filter(ins -> !ins.tags().isEmpty()
                                && ins.tags().stream().anyMatch(tag -> tag.key().toLowerCase().equals("name")
                                && tag.value().toLowerCase().contains("worker"))
                                && !ins.state().name().equals(InstanceStateName.TERMINATED)
                        )
                        .map(Instance::instanceId)
                        .collect(Collectors.toList()));
            }
            if (!CollectionUtils.isNullOrEmpty(workerInstanceIds)) {
                log.info("killing {} workers!", workerInstanceIds.size());
                TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                        .instanceIds(workerInstanceIds)
                        .build();
                ec2.terminateInstances(request);
                log.info("killed {} workers!", workerInstanceIds.size());
            }
            List<String> managerIds = new ArrayList<>();
            for (Reservation reservation : response.reservations()) {
                managerIds.addAll(reservation.instances().stream()
                        .filter(ins -> !ins.tags().isEmpty()
                                && ins.tags().stream().anyMatch(tag -> tag.key().toLowerCase().equals("name")
                                && tag.value().toLowerCase().contains("manager"))
                                && !ins.state().name().equals(InstanceStateName.TERMINATED)
                        )
                        .map(Instance::instanceId)
                        .collect(Collectors.toList()));

            }
            if (!CollectionUtils.isNullOrEmpty(managerIds)) {
                log.info("killing {} managers!", managerIds.size());
                TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                        .instanceIds(managerIds)
                        .build();
                ec2.terminateInstances(request);
                log.info("killed {} managers!", managerIds.size());
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
