import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.io.RandomAccessFile;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.PDFText2HTML;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class Utils {

    private static final String LOCAL_COPY_NAME = "local_copy";

    public static String convertPdfToImage(String url) throws IOException {
        InputStream in = new URL(url).openStream();
        Files.copy(in, Paths.get(LOCAL_COPY_NAME + ".pdf"), StandardCopyOption.REPLACE_EXISTING);

        PDDocument document = PDDocument.load(new File(LOCAL_COPY_NAME + ".pdf"));
        PDFRenderer pdfRenderer = new PDFRenderer(document);
        int pageCounter = 0;
        BufferedImage bim = pdfRenderer.renderImageWithDPI(
                pageCounter, 300, ImageType.RGB);
        ImageIOUtil.writeImage(
                bim, LOCAL_COPY_NAME + "-" + (pageCounter) + ".png", 300);
        document.close();
        return LOCAL_COPY_NAME + "-" + (pageCounter) + ".png";
    }

    public static String convertPdfToText(String url) throws IOException {
        InputStream in = new URL(url).openStream();
        Files.copy(in, Paths.get(LOCAL_COPY_NAME + ".pdf"), StandardCopyOption.REPLACE_EXISTING);

        File file = new File(LOCAL_COPY_NAME + ".pdf");
        String text;
        PDFParser parser = new PDFParser(new RandomAccessFile(file, "r")); //read mode
        parser.parse();
        COSDocument cosDocument = parser.getDocument();
        PDFTextStripper pdfTextStripper = new PDFTextStripper();
        PDDocument pdDocument = new PDDocument(cosDocument);
        text = pdfTextStripper.getText(pdDocument);
        PrintWriter pw = new PrintWriter(LOCAL_COPY_NAME + ".txt");
        pw.print(text);
        pw.close();
        cosDocument.close();
        return LOCAL_COPY_NAME + ".txt";
    }

    public static String convertPdfToHtml(String url) throws IOException {
        InputStream in = new URL(url).openStream();
        Files.copy(in, Paths.get(LOCAL_COPY_NAME + ".pdf"), StandardCopyOption.REPLACE_EXISTING);

        PDDocument pdDocument = PDDocument.load(new File(LOCAL_COPY_NAME + ".pdf"));
        PDFText2HTML textStripper = new PDFText2HTML();
        PrintWriter output = new PrintWriter(LOCAL_COPY_NAME + ".html", "utf-8");
        textStripper.writeText(pdDocument, output);
        output.close();
        return LOCAL_COPY_NAME + ".html";
    }
}
