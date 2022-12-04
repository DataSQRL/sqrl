package com.datasqrl.cmd;

import picocli.CommandLine;

@CommandLine.Command(name = "compile", description = "Compiles an SQRL script")
public class CompilerCommand extends AbstractCompilerCommand {

    protected CompilerCommand() {
        super(false);
    }

}
