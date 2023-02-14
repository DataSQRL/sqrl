package com.datasqrl;

import com.datasqrl.cmd.RootCommand;
import com.datasqrl.util.data.Quickstart;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.nio.file.Path;

public class ExamplesCmdTest {

    @Test
    public void testRootCmd() {
        execute(Quickstart.BASE_PATH.resolve("teaser"));
    }

    @Test
    public void testQuickStart() {
        execute(Quickstart.BASE_PATH.resolve("teaser"), "run","quickstart.sqrl");
    }

    public static void execute(Path rootDir, String... args) {
        new CommandLine(new RootCommand(rootDir)).execute(args);
    }

}
