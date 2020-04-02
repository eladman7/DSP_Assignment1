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
        System.out.println("worker: got new pdf message");
        handleNewPDFTask(message, outputQName, inputQName);
    }

    /**
     * process new pdf task on given url
     * upload output file to s3
     * send completed message to outputQName
     * delete handled message from inputQName
     *
     * @param message
     * @param outputQName
     * @param inputQName
     */
    private static void handleNewPDFTask(Message message, String outputQName, String inputQName) {
        String[] operationUrlPair = message.body().split("\t");
        String operationName = operationUrlPair[0].toUpperCase();
        String pdfS3PathToProcess = operationUrlPair[1];
        System.out.println("worker: handling message");
        System.out.println("worker: message body - operation name: " + operationName + ", pdf url:  " + pdfS3PathToProcess);
        String outFilePath;
        try {
            outFilePath = processOperation(operationName, pdfS3PathToProcess);
            String fileKey = "output";
            String bucket = S3Utils.uploadFile(outFilePath, fileKey);
            String remoteOutputURL = "https://" + bucket + ".s3.amazonaws.com/" + fileKey;
            SQSUtils.sendMSG(outputQName, buildCompletedMessage(operationName, pdfS3PathToProcess, remoteOutputURL));
        } catch (Exception e) {
            handleFailure(e, pdfS3PathToProcess, operationName);
        } finally {
            SQSUtils.deleteMSG(message, inputQName);
        }

    }

    private static String processOperation(String operationName, String pdfS3PathToProcess) throws IOException {
        String outFilePath;
        if (operationName.equals(PDFOperationType.TOIMAGE.name())) {
            outFilePath = Utils.convertPdfToImage(pdfS3PathToProcess);
        } else if (operationName.equals(PDFOperationType.TOHTML.name())) {
            outFilePath = Utils.convertPdfToHtml(pdfS3PathToProcess);
        } else {
            outFilePath = Utils.convertPdfToText(pdfS3PathToProcess);
        }
        return outFilePath;
    }

    private static String buildCompletedMessage(String operationName, String inputFileURL, String remoteOutputURL) {
        return operationName + ": " + inputFileURL + " " + remoteOutputURL;
    }

    private static String buildFailedMessage(Exception e, String inputFile, String opName) {
        return opName + ": " + inputFile + " " + e.getMessage();
    }

    private static void handleFailure(Exception e, String inputFile, String opName) {
        System.out.println("error on pdf task");
        SQSUtils.sendMSG(WORKER_OUTPUT_QUEUE, buildFailedMessage(e, inputFile, opName));
    }
}
