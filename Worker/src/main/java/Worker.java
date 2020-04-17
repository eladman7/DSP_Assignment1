import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.io.IOException;

public class Worker {
    private final static Logger log = LoggerFactory.getLogger(Worker.class);

    public static void main(String[] args) throws InterruptedException {
        String inputQName = args[0];
        String outputQNamePrefix = args[1];
        String outputQName;
        while (true) {
            try {

                Message message = SQSUtils.recieveMSG(inputQName);
                if (message != null) {
                    if (message.body().toLowerCase().equals("terminate")) {
                        log.info("worker: shutting down... goodbye");
                        break;
                    }
                    log.debug(message.body());
                    String appId = extractOutQName(message);
                    outputQName = outputQNamePrefix + appId;
                    handleNewPDFTask(message, outputQName, inputQName, appId);
                }
            } catch (SqsException | SdkClientException sqsExecption) {
                log.error("Worker.main(): got SqsException... " + sqsExecption.getMessage() +
                        "\nsleeping & retrying!");
                Thread.sleep(1000);
            }
        }
    }

    private static String extractOutQName(Message message) {
        return message.body().split("\\s+")[2];
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
    private static void handleNewPDFTask(Message message, String outputQName, String inputQName, String appId) {
        String[] operationUrlPair = message.body().split("\\s+");
        String operationName = operationUrlPair[0].toUpperCase();
        String pdfS3PathToProcess = operationUrlPair[1];
        log.debug("worker: message body - operation name: " + operationName + ", pdf url:  " + pdfS3PathToProcess);
        String outFilePath;
        try {
            outFilePath = processOperation(operationName, pdfS3PathToProcess);
            String fileKey = appId + "/" + "output" + String.valueOf(System.currentTimeMillis());
            String bucket = S3Utils.uploadFile(outFilePath, fileKey);
            String remoteOutputURL = "https://" + bucket + ".s3.amazonaws.com/" + fileKey;
            SQSUtils.sendMSG(outputQName, buildCompletedMessage(operationName, pdfS3PathToProcess, remoteOutputURL));
        } catch (Exception e) {
            handleFailure(e, pdfS3PathToProcess, operationName, outputQName);
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

    private static void handleFailure(Exception e, String inputFile, String opName, String outQName) {
        log.warn("Failure: " + e);
        SQSUtils.sendMSG(outQName, buildFailedMessage(e, inputFile, opName));
    }
}
