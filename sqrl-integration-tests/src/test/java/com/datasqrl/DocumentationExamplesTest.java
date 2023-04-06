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
import lombok.AllArgsConstructor;
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

    @ParameterizedTest
    @Disabled("port binding issues")
    @ArgumentsSource(RunProvider.class)
    public void runTutorials(@NonNull Path root, @NonNull String script, String graphQL) {
        execute(root, "run", script, graphQL);
    }

    @Test
    @Disabled
    public void runIndividual() {
        runTutorials(Clickstream.INSTANCE.getRootPackageDirectory(), "clickstream-teaser-docs.sqrl", null);
    }

    public static final TestCase[] CASES = {
        Execution.BOTH.of(Quickstart.INSTANCE,"quickstart-teaser.sqrl"),
        Execution.RUN.of(Quickstart.INSTANCE,"quickstart-teaser.sqrl", "quickstart-teaser.graphqls"),
        Execution.COMPILE.of(Quickstart.INSTANCE,"quickstart-sqrl.sqrl"),
        Execution.COMPILE.of(Quickstart.INSTANCE,"quickstart-user.sqrl"),
        Execution.BOTH.of(Quickstart.INSTANCE,"quickstart-user.sqrl", "quickstart-user-paging.graphqls"),
        Execution.BOTH.of(Quickstart.INSTANCE,"quickstart-export.sqrl"),
        Execution.COMPILE.of(Quickstart.INSTANCE,"quickstart-docs.sqrl"),
        Execution.BOTH.of(Sensors.INSTANCE,"sensors-teaser-docs.sqrl"),
        Execution.BOTH.of(Clickstream.INSTANCE,"clickstream-teaser-docs.sqrl"),
    };

    @AllArgsConstructor
    abstract static class ExecutionProvider implements ArgumentsProvider {

        final Execution execution;

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
            throws Exception {
            Path root = Quickstart.INSTANCE.getRootPackageDirectory();
            return Arrays.stream(CASES)
                .filter(c -> c.getExecution().matches(execution))
                .map( c -> Arguments.of(c.rootDir, c.script,
                        Strings.isNullOrEmpty(c.graphQLSchema)?null:c.graphQLSchema)
                );
        }
    }

    static class CompileProvider extends ExecutionProvider {

        public CompileProvider() {
            super(Execution.COMPILE);
        }
    }

    static class RunProvider extends ExecutionProvider {

        public RunProvider() {
            super(Execution.RUN);
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
        @NonNull Execution execution;

    }

    public enum Execution {
        COMPILE, RUN, BOTH;

        public TestCase of(UseCaseExample example, String script, String graphQLSchema) {
            return new TestCase(example.getRootPackageDirectory(), script, graphQLSchema, this);
        }

        public TestCase of(UseCaseExample example, String script) {
            return of(example, script, null);
        }

        public boolean matches(Execution other) {
            if (other==BOTH || this==BOTH) return true;
            return this==other;
        }
    }

}
