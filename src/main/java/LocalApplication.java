import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;

public class LocalApplication {


    public static void main(String[]args) {

        String input_file_name = args[0];
        int numOfPdfPerWorker = Integer.parseInt(args[1]);
        String terMessage = args[2];
        File input_file = new File(input_file_name);



        //upload input file to s3
        //uploadPdfFilesToS3(input_file, s3);

        S3Client s3;
        Region region = Region.US_WEST_2;
        s3 = S3Client.builder().region(region).build();


        String bucket = "bucket" + System.currentTimeMillis();
        String key = "inputFile";


        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .locationConstraint(region.id())
                                .build())
                .build());


        System.out.println(bucket);
        s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromFile(input_file));




    }

    private static void uploadPdfFilesToS3(File input_file, S3Client s3) {}




}
