package ai.datasqrl.physical.stream.inmemory.io;

import ai.datasqrl.io.impl.file.DirectoryDataSystem;
import ai.datasqrl.io.impl.file.FilePath;
import ai.datasqrl.io.sources.dataset.TableConfig;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

public class FileStreamUtil {

    public static Stream<String> filesByline(Path... paths) {
        return filesByline(Arrays.stream(paths));
    }

    public static Stream<String> filesByline(Stream<Path> paths) {
        Preconditions.checkArgument(paths!=null);
        return paths.flatMap(p -> {
            try {
                return Files.lines(p);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static Stream<Path> matchingFiles(Path start, DirectoryDataSystem.Connector directorySource,
            TableConfig table) throws IOException {
        return Files.find(start,100,
                (filePath, fileAttr) -> {
                    if (!fileAttr.isRegularFile()) return false;
                    if (fileAttr.size()<=0) return false;
                    return directorySource.isTableFile(FilePath.fromJavaPath(filePath), table);
                });

    }



}
