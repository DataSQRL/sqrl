package com.datasqrl;

import com.datasqrl.cmd.RootCommand;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.data.Quickstart;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class QuickstartTest {

    final Path root = Quickstart.INSTANCE.getRootPackageDirectory();

    @Test
    public void testRootCmd() {
        execute(root);
    }

    @Test
    public void testQuickStartTeaser() {
        execute(root, "run",root.resolve("quickstart-teaser.sqrl").toString(),
                root.resolve("quickstart-teaser.graphqls").toString() );
                //,"-a","graphql");
    }

    @Test
    @Disabled("only used to generate schema")
    public void getQuickstartGraphQL() {
        execute(root, "compile",root.resolve("quickstart-teaser.sqrl").toString(),
            "-a","graphql");
    }

    @Test
    public void testQuickStartUserWithGraphQL() {
        execute(root, "run",root.resolve("quickstart-user.sqrl").toString(),
            root.resolve("quickstart-user-paging.graphqls").toString() );
        //,"-a","graphql");
    }

    @ParameterizedTest
    @ArgumentsSource(TutorialProvider.class)
    public void compileQuickstartTutorial(Path root, String script) {
        execute(root, "compile", script);
        //   , root.resolve("quickstart-teaser.graphqls").toString() );
        //,"-a","graphql");
    }

    @Test
    public void runExportScript() {
        execute(root, "run", root.resolve(SCRIPTS[3]).toString());
    }

    @SneakyThrows
    @BeforeEach
    @AfterEach
    public void cleanUp() {
        //Clean up H2
        Files.deleteIfExists(Path.of("h2.db.mv.db"));
        //Clean up directory
        FileUtil.deleteDirectory(root.resolve("mysink-output").resolve("promotion"));
    }

    @SneakyThrows
    @BeforeEach
    public void createSinkDir() {
        Files.createDirectories(root.resolve("mysink-output"));
    }

    public static final String[] SCRIPTS = {
        "quickstart-teaser.sqrl",
        "quickstart-sqrl.sqrl",
        "quickstart-user.sqrl",
        "quickstart-export.sqrl",
        "quickstart-docs.sqrl"
        };

    static class TutorialProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
            throws Exception {
            Path root = Quickstart.INSTANCE.getRootPackageDirectory();
            return Arrays.stream(SCRIPTS)
                .map(s -> root.resolve(s).toString())
                .map(s->Arguments.of(root,s));
        }
    }

    public static void execute(Path rootDir, String... args) {
        new RootCommand(rootDir).getCmd().execute(args);
    }

}
