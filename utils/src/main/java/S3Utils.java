import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class S3Utils {

    private static final S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
//    public static final String PRIVATE_BUCKET = "dsp-private-bucket" + System.currentTimeMillis();
    public static final String PRIVATE_BUCKET = "dsp-private-bucket";
    public static final String PUBLIC_BUCKET = "dsp-public-bucket";

    // TODO: 08/04/2020 change isPrivate.
    // TODO: 09/04/2020 make public bucket once, only in the first local app
    public static String uploadFile(String fileLocalPath, String fileKey, boolean isPrivate) {
        uploadFile(fileLocalPath, fileKey, PUBLIC_BUCKET, isPrivate);
        return PUBLIC_BUCKET;
    }

    public static boolean uploadFile(String fileLocalPath, String fileKey, String bucketName, boolean isPrivate) {
        File input_file = new File(fileLocalPath);
        uploadInputFile(input_file, bucketName, fileKey, isPrivate);
        return true;
    }

    public static boolean uploadLargeFile(String fileLocalPath, String fileKey, String bucketName, boolean isPrivate) {
        multipartUpload(fileLocalPath, fileKey, bucketName, isPrivate);
        return true;
    }

    public static void getObjectToLocal(String fileKey, String bucket, String localFilePath) {
        s3.getObject(GetObjectRequest.builder().bucket(bucket).key(fileKey).build(),
                ResponseTransformer.toFile(Paths.get(localFilePath)));
    }

    /**
     * Upload first Input file to S3
     *
     */
    private static void uploadInputFile(File input_file, String bucket, String key, boolean isPrivate) {
        try {
            createBucket(bucket, isPrivate);
        } catch (BucketAlreadyExistsException ignored) {
            System.out.println("bucket with name: " + bucket + " already exists!");
        }
        if (!isPrivate)
            s3.putObject(PutObjectRequest.builder().acl(ObjectCannedACL.PUBLIC_READ_WRITE).bucket(bucket).key(key).build(),
                    RequestBody.fromFile(input_file));
        else
            s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                    RequestBody.fromFile(input_file));
    }

    private static void createBucket(String bucketName, boolean isPrivate) {
        if (!isPrivate) {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .acl(BucketCannedACL.PUBLIC_READ_WRITE)
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .build())
                    .build());
        } else
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .build())
                    .build());
    }

    private static void multipartUpload(String filePath, String keyName, String bucketName, boolean isPrivate) {
        createBucket(bucketName, isPrivate);
        File file = new File(filePath);
        long contentLength = file.length();
        long partSize = 5 * 1024 * 1024; // Set part size to 5 MB.

        try {
            // Create a list of ETag objects. You retrieve ETags for each object part uploaded,
            // then, after each individual part has been uploaded, pass the list of ETags to
            // the request to complete the upload.
            List<CompletedPart> completedParts = new ArrayList<>();
            CreateMultipartUploadRequest createMultipartUploadRequest;
            // Initiate the multipart upload.
            if (!isPrivate)
                createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .acl(ObjectCannedACL.PUBLIC_READ_WRITE)
                        .build();
            else
                createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .build();
            CreateMultipartUploadResponse response = s3.createMultipartUpload(createMultipartUploadRequest);
            String uploadId = response.uploadId();
            System.out.println(uploadId);

            // Upload the file parts.
            long filePosition = 0;
            System.out.println("content length: " + contentLength);
            for (int i = 1; filePosition < contentLength; i++) {
                System.out.println("uploading part: " + i);
                System.out.println("file pos: " + filePosition);
                // Because the last part could be less than 5 MB, adjust the part size as needed.
                partSize = Math.min(partSize, (contentLength - filePosition));

                // Create the request to upload a part.
                UploadPartRequest uploadPartRequestI = UploadPartRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .uploadId(uploadId)
                        .partNumber(i)
                        .contentLength(partSize)
                        .build();

                UploadPartResponse uploadPartResponse =
                        s3.uploadPart(uploadPartRequestI, RequestBody.fromInputStream(new FileInputStream(file), partSize));
                String etagI = uploadPartResponse.eTag();
                completedParts.add(CompletedPart.builder().partNumber(i).eTag(etagI).build());

                filePosition += partSize;
            }
            // Complete the multipart upload.
            CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(completedParts).build();
            CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .uploadId(uploadId)
                    .multipartUpload(completedMultipartUpload)
                    .build();
            s3.completeMultipartUpload(completeMultipartUploadRequest);
        } catch (SdkClientException | FileNotFoundException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
    }
}
