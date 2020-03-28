import software.amazon.awssdk.services.s3.S3Client;

public class LocalApplication {


    public static void main(String[]args) {

        String input_file = args[0];
        int numOfPdfPerWorker = Integer.parseInt(args[1]);
        String terMessage = args[2];
        uploadPdfFilesToS3(input_file);

        S3Client s3;




    }

    private static void uploadPdfFilesToS3(String input_file) {
    }


}
