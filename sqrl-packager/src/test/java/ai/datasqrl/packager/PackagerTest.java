package ai.datasqrl.packager;

import ai.datasqrl.packager.config.ConfigurationTest;
import ai.datasqrl.util.FileTestUtil;
import ai.datasqrl.util.SnapshotTest;
import ai.datasqrl.util.TestScript;
import ai.datasqrl.util.data.Retail;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class PackagerTest {

    SnapshotTest.Snapshot snapshot;

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        this.snapshot = SnapshotTest.Snapshot.of(getClass(),testInfo);
    }

    @Test
    @SneakyThrows
    public void testRetailPackaging() {
        Retail example = Retail.INSTANCE;
        Collection<Optional<Path>> graphqlschemas = List.of(Optional.empty(),
                Optional.of(example.getRootPackageDirectory().resolve("c360-full-graphqlv1").resolve("schema.graphqls")));
        List<TestScript> scripts = new ArrayList<>(example.getTestScripts().values());
        Collections.sort(scripts,(s1,s2)-> s1.getName().compareTo(s2.getName())); //Sort for deterministic iteration
        for (TestScript script : scripts) {
            for (Optional<Path> graphql : graphqlschemas) {
                Packager pkg = new Packager(script.getScriptPath(), graphql, ConfigurationTest.RESOURCE_DIR.resolve("package2.json"));
                pkg.inferDependencies();
                pkg.populateBuildDir();
                Path buildDir = script.getRootPackageDirectory().resolve(Packager.BUILD_DIR_NAME);
                String[] caseNames = {script.getName(), graphql.map(p -> p.toString()).orElse("none"), "dir"};
                snapshot.addContent(FileTestUtil.getAllFilesAsString(buildDir), caseNames);
                caseNames[2] = "package";
                snapshot.addContent(Files.readString(buildDir.resolve(Packager.PACKAGE_FILE_NAME)),caseNames);
                pkg.cleanUp();
            }
        }

        snapshot.createOrValidate();
    }



}
