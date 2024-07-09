package com.datasqrl.cmd;

import com.datasqrl.auth.AuthProvider;
import com.datasqrl.error.ErrorCollector;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

@CommandLine.Command(name = "run", description = "Lightweight sqrl script runner")
@Slf4j
public class RunCommand extends CompilerCommand {

}
