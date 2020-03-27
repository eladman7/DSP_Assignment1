import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

class UtilsTest {

    @org.junit.jupiter.api.Test
    void convertPdfToImage() {
        try {
            Utils.convertPdfToImage("/home/bar/IdeaProjects/Assignment1/src/main/resources/pdftoimage.pdf");
        } catch (IOException ex) {
            System.out.println("shit.. failed converting pdf to image");
            System.out.println(ex.toString());
        } finally {
            File file0 = new File("/home/bar/IdeaProjects/Assignment1/src/main/resources/pdftoimage.pdf-0.png");
            File file1 = new File("/home/bar/IdeaProjects/Assignment1/src/main/resources/pdftoimage.pdf-1.png");
            assertTrue(file0.exists());
            assertTrue(file1.exists());
        }
    }

    @org.junit.jupiter.api.Test
    void convertPdfToText() {

        try {
            Utils.convertPdfToText("/home/bar/IdeaProjects/Assignment1/src/main/resources/pdftotext.pdf");
        } catch (IOException ex) {
            System.out.println("shit.. failed converting pdf to text");
        } finally {
            File file0 = new File("/home/bar/IdeaProjects/Assignment1/src/main/resources/pdftotext.txt");
            assertTrue(file0.exists());
        }


    }

    @org.junit.jupiter.api.Test
    void convertPdfToHtml() {
        try {
            Utils.convertPdfToHtml("/home/bar/IdeaProjects/Assignment1/src/main/resources/pdftohtml.pdf");
        } catch (IOException ex) {
            System.out.println("shit.. failed converting pdf to html");
        } finally {
            File file = new File("/home/bar/IdeaProjects/Assignment1/src/main/resources/pdftohtml.html");
            assertTrue(file.exists());
        }
    }
}