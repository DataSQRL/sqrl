package com.datasqrl;

import com.datasqrl.cmd.RootCommand;
import com.datasqrl.util.data.Quickstart;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.nio.file.Path;

public class ExamplesCmdTest {

    @Test
    public void testRootCmd() {
        execute(Quickstart.BASE_PATH);
    }

    @Test
    @Disabled
    public void testQuickStart() {
        Path root = Quickstart.BASE_PATH;
        execute(root, "run",root.resolve("quickstart-teaser.sqrl").toString());
    }

    public static void execute(Path rootDir, String... args) {
        new CommandLine(new RootCommand(rootDir)).execute(args);
    }

}
