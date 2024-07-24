package com.datasqrl;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.cmd.RootCommand;
import com.datasqrl.cmd.StatusHook;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests some use cases in the test/resources/usecases folder using the `test` command.
 */
public class UseCasesIT {
  private static final Path RESOURCES = Paths.get("src/test/resources/usecases");


  @Test //Done
  public void testRepository() {
    execute("repository", "repo.sqrl", "repo.graphqls");
  }

//  @Test
  public void testLoggingToKafka() {
    execute("logging/it-kafka", "logging-kafka.sqrl", "logging-kafka.graphqls");
  }

  protected void execute(String path, String script, String graphql) {
    execute(path, script, graphql, null);
  }

  protected void execute(String path, String script, String graphql, String testSuffix) {
    Path rootDir = RESOURCES.resolve(path);
    List<String> argsList = new ArrayList<>();
    argsList.add("test");
    argsList.add(script);
    if (!Strings.isNullOrEmpty(graphql)) argsList.add(graphql);
    if (testSuffix!=null) {
      argsList.add("-s"); argsList.add("snapshots-"+testSuffix);
      argsList.add("--tests"); argsList.add("tests-"+testSuffix);
    }
    argsList.add("--profile");
    argsList.add(getProjectRoot(rootDir).resolve("profiles/default").toString());
    execute(rootDir, AssertStatusHook.INSTANCE, argsList.toArray(String[]::new));
  }

  protected void compile(String path, String script, String graphql) {
    Path rootDir = RESOURCES.resolve(path);
    List<String> argsList = new ArrayList<>();
    argsList.add("compile");
    argsList.add(script);
    if (!Strings.isNullOrEmpty(graphql)) argsList.add(graphql);
    argsList.add("--profile");
    argsList.add(getProjectRoot(rootDir).resolve("profiles/default").toString());
    execute(RESOURCES.resolve(path),
        AssertStatusHook.INSTANCE, argsList.toArray(a->new String[a]));
  }

  public static int execute(Path rootDir, StatusHook hook, String... args) {
    RootCommand rootCommand = new RootCommand(rootDir, hook);
    int exitCode = rootCommand.getCmd().execute(args) + (hook.isSuccess() ? 0 : 1);
    if (exitCode != 0) {
      fail();
    }
    return exitCode;
  }

  private Path getProjectRoot(Path script) {
    Path currentPath = script.toAbsolutePath();
    while (!currentPath.getFileName().toString().equals("sqrl-testing")) {
      currentPath = currentPath.getParent();
    }

    return currentPath.getParent();
  }
}
