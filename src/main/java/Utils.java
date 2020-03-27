import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.io.RandomAccessFile;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.PDFText2HTML;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

public class Utils {



    public static void convertPdfToImage(String filename) throws IOException {
        PDDocument document = PDDocument.load(new File(filename));
        PDFRenderer pdfRenderer = new PDFRenderer(document);
        int pageCounter = 0;
        for (PDPage page : document.getPages()) {
            BufferedImage bim = pdfRenderer.renderImageWithDPI(
                    pageCounter, 300, ImageType.RGB);
            ImageIOUtil.writeImage(
                    bim, filename + "-" + (pageCounter++) + ".png", 300);
        }
        document.close();
    }

    public static void convertPdfToText (String filename) throws IOException {
        File file = new File(filename);
        String text;
        PDFParser parser = new PDFParser(new RandomAccessFile(file, "r")); //read mode
        parser.parse();
        COSDocument cosDocument = parser.getDocument();
        PDFTextStripper pdfTextStripper = new PDFTextStripper();
        PDDocument pdDocument = new PDDocument(cosDocument);
        text = pdfTextStripper.getText(pdDocument);
        PrintWriter pw = new PrintWriter("src/main/resources/pdftotext.txt");
        pw.print(text);
        pw.close();
        cosDocument.close();
    }

    public static void convertPdfToHtml (String filename) throws IOException {
        PDDocument pdDocument = PDDocument.load(new File(filename));
        PDFText2HTML textStripper = new PDFText2HTML();
        PrintWriter output = new PrintWriter("src/main/resources/pdftohtml.html", "utf-8");
        textStripper.writeText(pdDocument, output);
        output.close();

    }
}
