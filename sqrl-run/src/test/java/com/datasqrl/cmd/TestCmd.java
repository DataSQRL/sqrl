package com.datasqrl.cmd;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.nio.file.Path;

public class TestCmd {

    @Test
    public void testNutshop() {

    }

    public static void execute(Path rootDir, String[] args) {
        new CommandLine(new RootCommand(rootDir)).execute(args);
    }

}
