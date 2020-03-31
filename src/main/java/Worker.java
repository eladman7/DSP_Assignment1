import software.amazon.awssdk.services.sqs.model.Message;

import java.io.IOException;

public class Worker {

    private final static String WORKER_OUTPUT_QUEUE = "WORKER_OUTPUT_Q";

    public static void main(String[] args) {
        String inputQName = args[0];
        String outputQName = args[1];
        while (true) {
            handleMessage(SQSUtils.recieveMSG(inputQName), outputQName, inputQName);
        }
    }

    private static void handleMessage(Message message, String outputQName, String inputQName) {
        // assuming only 1 type of message
        handleNewPDFTask(message, outputQName, inputQName);
    }

    /**
     * process new pdf task on given url
     * upload output file to s3
     * send completed message to outputQName
     * delete handled message from inputQName
     *  @param message
     * @param outputQName
     * @param inputQName
     */
    private static void handleNewPDFTask(Message message, String outputQName, String inputQName) {
        String[] operationUrlPair = message.body().split("\t");
        String operationName = operationUrlPair[0].toUpperCase();
        String pdfS3PathToProcess = operationUrlPair[1];
        String outFilePath = processOperation(operationName, pdfS3PathToProcess);
        String fileKey = "output";
        String bucket = S3Utils.uploadFile(outFilePath, fileKey);
//        https://<bucket-name>.s3.amazonaws.com/<key>   - remote output url
        String remoteOutputURL = "https://" + bucket + ".s3.amazonaws.com/" + fileKey;
        SQSUtils.sendMSG(outputQName, buildCompletedMessage(operationName, pdfS3PathToProcess, remoteOutputURL));
        SQSUtils.deleteMSG(message, inputQName);
    }

    private static String processOperation(String operationName, String pdfS3PathToProcess) {
        String outFilePath = "";
        if (operationName.equals(PDFOperationType.TOIMAGE.name())) {
            try {
                outFilePath = Utils.convertPdfToImage(pdfS3PathToProcess);
            } catch (IOException e) {
                handleFailure(e);
            }
        } else if (operationName.equals(PDFOperationType.TOHTML.name())) {
            try {
                outFilePath = Utils.convertPdfToHtml(pdfS3PathToProcess);
            } catch (IOException e) {
                handleFailure(e);
            }
        } else {
            try {
                outFilePath = Utils.convertPdfToText(pdfS3PathToProcess);
            } catch (IOException e) {
                handleFailure(e);
            }
        }
        return outFilePath;
    }

    private static String buildCompletedMessage(String operationName, String inputFileURL, String remoteOutputURL) {
        return operationName + ": " + inputFileURL + " " + remoteOutputURL;
    }

    private static void handleFailure(IOException e) {
        SQSUtils.sendMSG(WORKER_OUTPUT_QUEUE, e.getMessage());
    }
}
