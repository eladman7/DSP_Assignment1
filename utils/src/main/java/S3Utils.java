import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final static Logger log = LoggerFactory.getLogger(S3Utils.class);

    private static final S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
    public static final String PRIVATE_BUCKET = "eladoss";

    public static String uploadFile(String fileLocalPath, String fileKey) {
        uploadFile(fileLocalPath, fileKey, PRIVATE_BUCKET);
        return PRIVATE_BUCKET;
    }

    public static boolean uploadFile(String fileLocalPath, String fileKey, String bucketName) {
        File input_file = new File(fileLocalPath);
        uploadFile(input_file, bucketName, fileKey);
        return true;
    }

    public static boolean uploadLargeFile(String fileLocalPath, String fileKey, String bucketName) {
        multipartUpload(fileLocalPath, fileKey, bucketName);
        return true;
    }

    //todo: elad maybe add retry
    public static void getObjectToLocal(String fileKey, String bucket, String localFilePath) {
        s3.getObject(GetObjectRequest.builder().bucket(bucket).key(fileKey).build(),
                ResponseTransformer.toFile(Paths.get(localFilePath)));
    }

    /**
     * Upload first Input file to S3
     */
    public static void uploadFile(File input_file, String bucket, String key) {
        try {
            createBucket(bucket);
        } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException ignored) {
            log.debug("bucket with name: " + bucket + " already exists!");
        }
        s3.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build(),
                RequestBody.fromFile(input_file));
    }

    private static void createBucket(String bucketName) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucketName)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .build())
                .build());
        PutPublicAccessBlockRequest pubBlockReq = PutPublicAccessBlockRequest.builder()
                .bucket(bucketName)
                .publicAccessBlockConfiguration(PublicAccessBlockConfiguration.builder()
                        .blockPublicAcls(true)
                        .blockPublicPolicy(true)
                        .ignorePublicAcls(true)
                        .restrictPublicBuckets(true)
                        .build())
                .build();
        try {
            s3.putPublicAccessBlock(pubBlockReq);
        } catch (S3Exception | SdkClientException ex) {
            log.warn("could not make bucket not public");
        }
    }

    private static void multipartUpload(String filePath, String keyName, String bucketName) {
        createBucket(bucketName);
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
            createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .build();
            CreateMultipartUploadResponse response = s3.createMultipartUpload(createMultipartUploadRequest);
            String uploadId = response.uploadId();
            log.debug(uploadId);

            // Upload the file parts.
            long filePosition = 0;
            log.debug("content length: " + contentLength);
            for (int i = 1; filePosition < contentLength; i++) {
                log.debug("uploading part: " + i);
                log.debug("file pos: " + filePosition);
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

    /**
     * Extract the file url from some s3 path
     *
     * @param key of the bucket
     * @return file url
     */
    public static String getFileUrl(String key) {
        return "s3://" + S3Utils.PRIVATE_BUCKET + "/" + key;
    }
}
