package ai.datasqrl.util;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.util.Strings;
import org.junit.jupiter.api.TestInfo;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public class SnapshotTest {

    public static final String[] BASE_SNAPSHOT_DIR = new String[]{"src","test","resources","snapshots"};
    public static final String SNAPSHOT_EXTENSION = ".txt";


    public static void createOrValidateSnapshot(@NonNull String className, @NonNull String fileName, @NonNull String content) {
        new Snapshot(className,fileName,new StringBuilder(content)).createOrValidate();
    }


    private static Path getPath(String[] components) {
        return Paths.get(components[0], Arrays.copyOfRange(components,1,components.length));
    }

    @Value
    public static class Snapshot {

        public static final String CONTENT_DELIMITER = "\n";
        public static final String HEADER_PREFIX = ">>>";
        public static final String HEADER_DELIMITER = "-";
        public static final String HEADER_SUFFIX = "\n";

        String className;
        String fileName;
        StringBuilder content;

        public static Snapshot of(@NonNull Class testClass, @NonNull TestInfo testInfo, String content) {
            String fileName = testInfo.getDisplayName();
            if (fileName.endsWith("()")) fileName = fileName.substring(0,fileName.length()-2);
            StringBuilder c = new StringBuilder();
            if (Strings.isNotEmpty(content)) c.append(content);
            return new Snapshot(testClass.getName(), fileName, c);
        }

        public static Snapshot of(@NonNull Class testClass, @NonNull TestInfo testInfo) {
            return of(testClass,testInfo,null);
        }

        public String getContent() {
            return content.toString();
        }

        public boolean hasContent() {
            return content.length()>0;
        }

        public Snapshot addContent(@NonNull String addedContent, String... caseNames) {
            if (caseNames!=null && caseNames.length>0) {
                //Add header
                int j = 0;
                for (String caseName : caseNames) {
                    if (j++ == 0) content.append(HEADER_PREFIX);
                    else content.append(HEADER_DELIMITER);
                    content.append(caseName);
                }
                content.append(HEADER_SUFFIX);
            }
            content.append(addedContent).append(CONTENT_DELIMITER);
            return this;
        }

        @SneakyThrows
        public void createOrValidate() {
            String content = getContent();
            Preconditions.checkArgument(fileName.matches("^\\w+$"), "Invalid display name: %s", fileName);
            Preconditions.checkArgument(Strings.isNotEmpty(className), "No snapshot class name");
            Preconditions.checkArgument(Strings.isNotEmpty(content), "No snapshot content");

            String[] snapLocation = ArrayUtils.addAll(BASE_SNAPSHOT_DIR,className.split("\\."));
            snapLocation = ArrayUtils.addAll(snapLocation,fileName+SNAPSHOT_EXTENSION);
            Path path = getPath(snapLocation);
            if (!Files.exists(path)) {
                Files.createDirectories(path.getParent());
                log.info("Test not running, creating snapshot");
                Files.write(path, content.getBytes());
                fail("Creating snapshots");
            } else {
                byte[] data = Files.readAllBytes(path);
                String dataStr = new String(data);
                assertEquals(dataStr, content, fileName);
            }
        }

    }


}
