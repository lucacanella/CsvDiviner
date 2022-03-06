import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class TestUtils {


    /**
     * Byte to byte files compare
     * https://www.baeldung.com/java-compare-files
     * @param path1 Path to the first file
     * @param path2 Path to the second file
     * @return the position of the last read byte, or -1 if files contents are equal
     */
    public static long compareFilesByteToByte(String path1, String path2) throws IOException {
        try (
                BufferedInputStream fis1 = new BufferedInputStream(new FileInputStream(new File(path1)));
                BufferedInputStream fis2 = new BufferedInputStream(new FileInputStream(new File(path2)))
        ) {
            int ch = 0, ch2 = -1;
            long pos = 1;
            while ((ch = fis1.read()) != -1) {
                ch2 = fis2.read();
                if (ch != ch2) {
                    return pos;
                }
                pos++;
            }
            if (fis2.read() == -1) {
                return -1;
            } else {
                return pos;
            }
        }
    }

}
