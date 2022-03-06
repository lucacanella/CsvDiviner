import org.junit.Test;
import org.lucacanella.csvdiviner.CLIAnalyzerMode;

import java.io.IOException;

public class CLIAnalyzerModeTest {
    CLIAnalyzerMode cliAnalyzer;

    static final String CLI_ANALYZER_MODE_TEST_INPUT_FILE = "./test-resources/analysis_test_1.csv";
    static final String CLI_ANALYZER_MODE_TEST_RESULT_FILE = "./test-resources/analysis_test_1.csv.json";
    static final String CLI_ANALYZER_MODE_TEST_VERIFIED_FILE = "./test-resources/analysis_test_1.csv.json.ver";

    /**
     * Run analyzer on a test file and check if result matches a pre-verified version
     *  - small file test version
     * @throws IOException
     */
    @Test
    public void cliAnalyzerModeTest01() throws IOException {
        cliAnalyzer = new CLIAnalyzerMode();
        cliAnalyzer.execute(new String[]{
                "-c", "UTF-8",
                "-l", "INFO",
                "-o", CLI_ANALYZER_MODE_TEST_RESULT_FILE,
                "--printStats",
                "-q", "\"",
                "-s", ",",
                "--showProgress",
                CLI_ANALYZER_MODE_TEST_INPUT_FILE
        });
        long diffRes = TestUtils.compareFilesByteToByte(CLI_ANALYZER_MODE_TEST_RESULT_FILE, CLI_ANALYZER_MODE_TEST_VERIFIED_FILE);
        assert diffRes == -1;
    }

    static final String CLI_ANALYZER_MODE_TEST2_INPUT_FILE = "./test-resources/analysis_test_2.csv";
    static final String CLI_ANALYZER_MODE_TEST2_RESULT_FILE = "./test-resources/analysis_test_2.csv.json";
    static final String CLI_ANALYZER_MODE_TEST2_VERIFIED_FILE = "./test-resources/analysis_test_2.csv.json.ver";

    /**
     * Run analyzer on a test file and check if result matches a pre-verified version
     *  - 72k rows file test version
     * @throws IOException
     */
    @Test
    public void cliAnalyzerModeTest02() throws IOException {
        cliAnalyzer = new CLIAnalyzerMode();
        cliAnalyzer.execute(new String[]{
                "-c", "UTF-8",
                "-l", "WARNING",
                "-o", CLI_ANALYZER_MODE_TEST2_RESULT_FILE,
                "-q", "\"",
                "-s", ",",
                CLI_ANALYZER_MODE_TEST2_INPUT_FILE
        });
        long diffRes = TestUtils.compareFilesByteToByte(CLI_ANALYZER_MODE_TEST2_RESULT_FILE, CLI_ANALYZER_MODE_TEST2_VERIFIED_FILE);
        assert diffRes == -1;
    }

}
