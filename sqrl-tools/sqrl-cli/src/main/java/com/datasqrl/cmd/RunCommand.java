package com.datasqrl.cmd;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

@CommandLine.Command(name = "run", description = "Lightweight sqrl script runner")
@Slf4j
public class RunCommand extends CompilerCommand {

}
