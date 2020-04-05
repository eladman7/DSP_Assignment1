import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;

public class S3Utils {

    private static final S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();

//    public static String uploadFile(String fileLocalPath, String fileKey) {
//        String bucketName = "bucket" + System.currentTimeMillis();
//        uploadFile(fileLocalPath, fileKey, bucketName);
//        return bucketName;
//    }
    public static String uploadFile(String fileLocalPath, String fileKey) {
        String bucketName = "dsp-public-bucket";
        uploadFile(fileLocalPath, fileKey, bucketName);
        return bucketName;
    }

    public static boolean uploadFile(String fileLocalPath, String fileKey, String bucketName) {
        File input_file = new File(fileLocalPath);
        uploadInputFile(input_file, bucketName, fileKey);
        return true;
    }

    /**
     * Upload first Input file to S3
     *
     * @param input_file
     * @param bucket
     * @param key
     */
    private static void uploadInputFile(File input_file, String bucket, String key) {
       try {
           s3.createBucket(CreateBucketRequest
                   .builder()
                   .acl(BucketCannedACL.PUBLIC_READ_WRITE)
                   .bucket(bucket)
                   .createBucketConfiguration(
                           CreateBucketConfiguration.builder()
                                   .build())
                   .build());
       } catch (BucketAlreadyExistsException ignored) {}
        s3.putObject(PutObjectRequest.builder().acl(ObjectCannedACL.PUBLIC_READ_WRITE).bucket(bucket).key(key).build(),
                RequestBody.fromFile(input_file));
    }

//    private static void uploadInputFile(File input_file, String bucket, String key) {
//        s3.createBucket(CreateBucketRequest
//                .builder()
//                .acl(BucketCannedACL.PUBLIC_READ_WRITE)
//                .bucket(bucket)
//                .createBucketConfiguration(
//                        CreateBucketConfiguration.builder()
//                                .build())
//                .build());
//        s3.putObject(PutObjectRequest.builder().acl(ObjectCannedACL.PUBLIC_READ_WRITE).bucket(bucket).key(key).build(),
//                RequestBody.fromFile(input_file));
//    }
}
