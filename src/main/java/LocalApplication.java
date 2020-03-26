import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.io.RandomAccessFile;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageTree;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.PDFText2HTML;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;

public class LocalApplication {


    public static void main(String[]args) {

        try {
            convertPdfToImage("/home/bar/IdeaProjects/Assignment1/src/main/resources/pdftoimage.pdf");
        } catch (IOException ex) {
            System.out.println("shit.. failed converting pdf to image");
            System.out.println(ex.toString());
        }
        try {
            convertPdfToText("/home/bar/IdeaProjects/Assignment1/src/main/resources/pdftotext.pdf");
        } catch (IOException ex) {
            System.out.println("shit.. failed converting pdf to text");
        }

        try {
            convertPdfToHtml("/home/bar/IdeaProjects/Assignment1/src/main/resources/pdftohtml.pdf");
        } catch (IOException ex) {
            System.out.println("shit.. failed converting pdf to html");
        }


    }


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
        PrintWriter pw = new PrintWriter("src/main/resources/Convertedtext.txt");
        pw.print(text);
        pw.close();
    }

    public static void convertPdfToHtml (String filename) throws IOException {
        PDDocument pdDocument = PDDocument.load(new File(filename));
        PDFText2HTML textStripper = new PDFText2HTML();
        PrintWriter output = new PrintWriter("src/main/resources/Convertedhtml.html", "utf-8");
        textStripper.writeText(pdDocument, output);
        output.close();

    }

}
