package com.datasqrl;

import com.datasqrl.cmd.RootCommand;
import com.datasqrl.util.data.Quickstart;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class QuickstartTest {

    @Test
    public void testRootCmd() {
        execute(Quickstart.BASE_PATH);
    }

    @Test
    @Disabled
    public void testQuickStartTeaser() {
        Path root = Quickstart.BASE_PATH;
        execute(root, "run",root.resolve("quickstart-teaser.sqrl").toString(),
                root.resolve("quickstart-teaser.graphqls").toString() );
                //,"-a","graphql");
    }

    @Disabled
    @ParameterizedTest
    @ArgumentsSource(TutorialProvider.class)
    public void testQuickstartTutorial(Path root, String script) {
        execute(root, "compile", script);
        //   , root.resolve("quickstart-teaser.graphqls").toString() );
        //,"-a","graphql");
    }

    @SneakyThrows
    @BeforeEach
    @AfterEach
    public void cleanUpH2() {
        Files.deleteIfExists(Path.of("h2.db.mv.db"));
    }

    static class TutorialProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
            throws Exception {
            Path root = Quickstart.BASE_PATH;
            String[] scripts = { root.resolve("quickstart-docs.sqrl").toString() ,
                root.resolve("quickstart-basic.sqrl").toString(),
                root.resolve("quickstart-user.sqrl").toString(),
                root.resolve("quickstart-export.sqrl").toString()};
            return Arrays.stream(scripts).map(s->Arguments.of(root,s));
        }
    }

    public static void execute(Path rootDir, String... args) {
        new RootCommand(rootDir).getCmd().execute(args);
    }

}
