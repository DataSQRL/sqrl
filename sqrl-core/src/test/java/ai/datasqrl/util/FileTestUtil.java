package ai.datasqrl.util;

import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class FileTestUtil {

    @SneakyThrows
    public static int countLinesInAllFiles(Path path) {
        int lineCount = 0;
        for (File file : FileUtils.listFiles(path.toFile(),new RegexFileFilter("^part(.*?)"),
                DirectoryFileFilter.DIRECTORY)) {
            try (Stream<String> stream = Files.lines(file.toPath(), StandardCharsets.UTF_8)) {
                lineCount += stream.count();
            }
        }
        return lineCount;
    };

}
