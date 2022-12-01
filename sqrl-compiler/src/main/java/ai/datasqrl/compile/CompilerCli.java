package ai.datasqrl.compile;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.error.ErrorPrinter;
import java.nio.file.Path;
import java.util.Optional;
import lombok.SneakyThrows;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public class CompilerCli implements Runnable {
  @Parameters(index = "0", description = "Build directory")
  private Optional<Path> buildDir = Optional.empty();

  @Option(names = {"-s", "--schema"}, description = "Schemas")
  private Optional<String> graphqlSchemas = Optional.empty();

  @Option(names = {"-h", "--help"}, usageHelp = true,
      description = "Displays this help message and quits.")
  private boolean helpRequested = false;

  public static void main(String[] args) {
    new CommandLine(new CompilerCli()).execute(args);
  }

  @SneakyThrows
  public void run() {
    Path buildPath = buildDir.map(e->e.resolve("build"))
        .orElseGet(() -> Path.of("./build"));
    Optional<Path> schema =
        graphqlSchemas.map(s->buildPath.resolve(s));

    Compiler compiler = new Compiler();
    ErrorCollector errorCollector = ErrorCollector.root();
    compiler.run(errorCollector, buildPath, schema, null);

    if (errorCollector.hasErrors()) {
      System.out.println(ErrorPrinter.prettyPrint(errorCollector));
    }
  }
}
