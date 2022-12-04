package com.datasqrl.compile;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import lombok.SneakyThrows;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.util.Optional;

public class CompilerCli implements Runnable {

  @Parameters(index = "0", description = "Base directory")
  private Optional<Path> buildDir = Optional.empty();

  @Option(names = {"-h", "--help"}, usageHelp = true,
      description = "Displays this help message and quits.")
  private boolean helpRequested = false;

  public static void main(String[] args) {
    new CommandLine(new CompilerCli()).execute(args);
  }

  @SneakyThrows
  public void run() {
    Path buildPath = buildDir.map(e -> e.resolve("build"))
        .orElseGet(() -> Path.of("./build"));

    Compiler compiler = new Compiler();
    ErrorCollector errorCollector = ErrorCollector.root();
    compiler.run(errorCollector, buildPath);

    if (errorCollector.hasErrors()) {
      System.out.println(ErrorPrinter.prettyPrint(errorCollector));
    }
  }
}
