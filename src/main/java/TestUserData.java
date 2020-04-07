import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.Base64;

public class TestUserData {
    public static void main(String[] args) {
        String amiId = "ami-076515f20540e6e0b";
//        ami-b66ed3de, ami-076515f20540e6e0b
        Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();
        String bucketName = "managercode";
        String fileKey = "managerapp";
//        S3Utils.uploadFile("/Users/eman/IdeaProjects/DSP-Assignment1/out/artifacts/Assignment1_jar/Assignment1.jar", fileKey, bucketName);
        //        https://<bucket-name>.s3.amazonaws.com/<key>   - remote output url
        String s3Path = "https://" + bucketName + ".s3.amazonaws.com/" + fileKey;

        String script = "#!/bin/bash\nwget " + s3Path + " -O done.jar\n" + "java -jar done.jar\n";

        System.out.println("user data: " + script);

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                .keyName("eladkey")
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("name")
                .value("manager")
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
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}
