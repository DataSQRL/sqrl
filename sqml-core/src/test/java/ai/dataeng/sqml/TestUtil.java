package ai.dataeng.sqml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

public class TestUtil {


    public static final Stream<String> files2StringByLine(Path... files) {
        return Arrays.stream(files).flatMap(f -> {
            try {
                return Files.readAllLines(f).stream();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
