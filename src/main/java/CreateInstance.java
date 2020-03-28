import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

/**
 * Creates an EC2 instance
 */
public class CreateInstance {
    public static void main(String[] args) {
        final String USAGE =
                "To run this example, supply an instance name and AMI image id\n" +
                        "Both values can be obtained from the AWS Console\n" +
                        "Ex: CreateInstance <instance-name> <ami-image-id>\n";
 
        if (args.length != 2) {
            System.out.println(USAGE);
            System.exit(1);
        }
 
        String name = args[0];
        String amiId = args[1];
 
        // snippet-start:[ec2.java2.create_instance.main]
        Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
 
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T1_MICRO)
                .maxCount(1)
                .minCount(1)
                .build();
 
        RunInstancesResponse response = ec2.runInstances(runRequest);
 
        String instanceId = response.instances().get(0).instanceId();
 
        Tag tag = Tag.builder()
                .key("Name")
                .value(name)
                .build();
 
        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();
 
        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully started EC2 instance %s based on AMI %s",
                    instanceId, amiId);
       
        } catch (Ec2Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        // snippet-end:[ec2.java2.create_instance.main]
        System.out.println("Done!");
    }
}
// snippet-end:[ec2.java2.create_instance.complete]