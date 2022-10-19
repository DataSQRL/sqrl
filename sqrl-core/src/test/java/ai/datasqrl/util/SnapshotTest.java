package ai.datasqrl.util;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
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

    public enum Execution {
        TESTING, CREATING;

        public void test() {
            if (this==CREATING) fail("Creating snapshots");
        }
    }

    public static Execution createOrValidateSnapshot(@NonNull String className, @NonNull String fileName, @NonNull String content) {
        return createOrValidateSnapshot(new SnapshotRun(className,fileName,true),content);
    }

    @SneakyThrows
    public static Execution createOrValidateSnapshot(@NonNull SnapshotTest.SnapshotRun snapshot, @NonNull String content) {
        String className = snapshot.className, fileName = snapshot.fileName;
        Preconditions.checkArgument(fileName.matches("^\\w+$"), "Invalid display name: %s", fileName);
        Preconditions.checkArgument(Strings.isNotEmpty(className));
        Preconditions.checkArgument(Strings.isNotEmpty(content));

        String[] snapLocation = ArrayUtils.addAll(BASE_SNAPSHOT_DIR,className.split("\\."));
        snapLocation = ArrayUtils.addAll(snapLocation,fileName+SNAPSHOT_EXTENSION);
        Path path = getPath(snapLocation);
        if (!Files.exists(path)) {
            Files.createDirectories(path.getParent());
            log.info("Test not running, creating snapshot");
            Files.write(path, content.getBytes());
            if (snapshot.failOnCreate) fail("Creating snapshots");
            return Execution.CREATING;
        } else {
            byte[] data = Files.readAllBytes(path);
            String dataStr = new String(data);
            assertEquals(dataStr, content, fileName);
            return Execution.TESTING;
        }
    }

    private static Path getPath(String[] components) {
        return Paths.get(components[0], Arrays.copyOfRange(components,1,components.length));
    }

    @Value
    public static class SnapshotRun {

        String className;
        String fileName;
        boolean failOnCreate;

        public static SnapshotRun of(@NonNull Class testClass, @NonNull TestInfo testInfo, String... caseNames) {
            String fileName = testInfo.getDisplayName();
            if (fileName.endsWith("()")) fileName = fileName.substring(0,fileName.length()-2);
            SnapshotRun sni = new SnapshotRun(testClass.getName(), fileName, true);
            if (caseNames!=null && caseNames.length>0) sni = sni.with(caseNames);
            return sni;
        }

        public static SnapshotRun of(@NonNull Class testClass, @NonNull TestInfo testInfo) {
            return of(testClass, testInfo, null);
        }

        public SnapshotRun with(String... caseNames) {
            String fileName = this.fileName;
            for (String caseName : caseNames) {
                if (Strings.isNotEmpty(caseName)) fileName += "_" + caseName;
            }
            return new SnapshotRun(className,fileName,failOnCreate);
        }

        public SnapshotRun with(boolean failOnCreate) {
            return new SnapshotRun(className,fileName,failOnCreate);
        }

    }


}
