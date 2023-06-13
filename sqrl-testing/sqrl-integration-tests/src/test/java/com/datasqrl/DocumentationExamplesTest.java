package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.cmd.RootCommand;
import com.datasqrl.cmd.StatusHook;
import com.datasqrl.cmd.StatusHook.Impl;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.data.Clickstream;
import com.datasqrl.util.data.Quickstart;
import com.datasqrl.util.data.Sensors;
import com.datasqrl.util.data.UseCaseExample;
import com.google.common.base.Strings;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * This tests the examples we use in our online documentation. It requires access to the repository.
 * The test only assures that the compile or run command executes successfully.
 * Any more in-depth testing should be done in the core or integration test suite.
 */
public class DocumentationExamplesTest {

    public static final String DEFAULT_SINK_DIR = "mysink-output";

    @Test
    public void testRootCmd() {
        StatusHook.Impl statusHook = new Impl();
        new RootCommand(Path.of(""),statusHook).getCmd().execute(new String[0]);
        assertTrue(statusHook.isSuccess());
    }

    @ParameterizedTest
    @ArgumentsSource(CompileProvider.class)
    public void compileTutorials(@NonNull Path root, @NonNull String script, String graphQL) {
        execute(root, "compile", script, graphQL);
    }


    @Test
    @Disabled
    public void runIndividual() {
        compileTutorials(Clickstream.INSTANCE.getRootPackageDirectory(), "clickstream-teaser-docs.sqrl", null);
    }

    public static final TestCase[] CASES = {
        TestCase.of(Quickstart.INSTANCE,"quickstart-frontpage.sqrl"),
        TestCase.of(Quickstart.INSTANCE,"quickstart-teaser.sqrl", "quickstart-teaser.graphqls"),
        TestCase.of(Quickstart.INSTANCE,"quickstart-sqrl.sqrl"),
        TestCase.of(Quickstart.INSTANCE,"quickstart-user.sqrl"),
        TestCase.of(Quickstart.INSTANCE,"quickstart-user.sqrl", "quickstart-user-paging.graphqls"),
        TestCase.of(Quickstart.INSTANCE,"quickstart-export.sqrl"),
        TestCase.of(Quickstart.INSTANCE,"quickstart-docs.sqrl"),
        TestCase.of(Sensors.INSTANCE,"sensors-teaser-docs.sqrl"),
        TestCase.of(Sensors.INSTANCE,"sensors-short.sqrl"),
        TestCase.of(Sensors.INSTANCE,"sensors-short.sqrl"),
        TestCase.of(Sensors.INSTANCE,"metrics-teaser.sqrl"),
        TestCase.of(Sensors.INSTANCE,"metrics-teaser.sqrl", "metricsapi-teaser.graphqls"),
        TestCase.of(Sensors.INSTANCE,"metrics-mutation.sqrl", "metricsapi.graphqls"),
        TestCase.of(Clickstream.INSTANCE,"clickstream-teaser-docs.sqrl"),
    };

    static class CompileProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
            throws Exception {
            return Arrays.stream(CASES)
                .map( c -> Arguments.of(c.rootDir, c.script,
                        Strings.isNullOrEmpty(c.graphQLSchema)?null:c.graphQLSchema)
                );
        }
    }

    private Path root;

    @SneakyThrows
    public void createSinkDir(Path rootDir) {
        Files.createDirectories(rootDir.resolve(DEFAULT_SINK_DIR));
    }

    public void execute(@NonNull Path rootDir, @NonNull String command, String script, String graphQL, String... options) {
        this.root = rootDir; //For clean up
        createSinkDir(rootDir);
        String[] args = new String[2+options.length+(graphQL==null?0:1)];
        int i = 0;
        args[i++] = command;
        args[i++] = root.resolve(script).toString();
        if (graphQL!=null) args[i++] = root.resolve(graphQL).toString();
        for (int j = 0; j < options.length; j++) {
            args[i++]=options[j];
        }
        StatusHook.Impl statusHook = new Impl();
        new RootCommand(rootDir,statusHook).getCmd().execute(args);
        assertTrue(statusHook.isSuccess());
    }

    @SneakyThrows
    @BeforeEach
    @AfterEach
    public void cleanUp() {
        //Clean up H2
        Files.deleteIfExists(Path.of("h2.db.mv.db"));
        //Clean up directory
        if (root!=null) FileUtil.deleteDirectory(root.resolve(DEFAULT_SINK_DIR));
    }

    @Value
    public static class TestCase {

        @NonNull Path rootDir;
        @NonNull String script;
        String graphQLSchema;

        public static TestCase of(UseCaseExample example, String script, String graphQLSchema) {
            return new TestCase(example.getRootPackageDirectory(), script, graphQLSchema);
        }

        public static TestCase of(UseCaseExample example, String script) {
            return of(example, script, null);
        }


    }

}
