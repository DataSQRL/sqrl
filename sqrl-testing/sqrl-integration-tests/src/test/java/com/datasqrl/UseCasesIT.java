package com.datasqrl;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.cmd.RootCommand;
import com.datasqrl.cmd.StatusHook;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests some use cases in the test/resources/usecases folder using the `test` command.
 */
@Disabled
public class UseCasesIT {
  protected static final Path PROJECT_ROOT = getProjectRoot();
  private static final Path RESOURCES = Paths.get("src/test/resources/usecases");


  @Test //Done
  public void testRepository() {
    execute("repository", "repo.sqrl", "repo.graphqls");
  }

  @Test
  public void testLoggingToKafka() {
    execute("logging/it-kafka", "logging-kafka.sqrl", "logging-kafka.graphqls");
  }

  public void execute(String path, String script, String graphql) {
    execute("test", path, script, graphql, null);
  }

  public void execute(String goal, String path, String script, String graphql, String testSuffix, String... args) {
    Path rootDir = RESOURCES.resolve(path);
    List<String> argsList = new ArrayList<>();
    argsList.add(goal);
    argsList.add(script);
    if (!Strings.isNullOrEmpty(graphql)) argsList.add(graphql);
    if (testSuffix!=null) {
      argsList.add("-s"); argsList.add("snapshots-"+testSuffix);
      argsList.add("--tests"); argsList.add("tests-"+testSuffix);
    }
    argsList.add("--profile");
    argsList.add(getProjectRoot(rootDir).resolve("profiles/default").toString());
    argsList.addAll(Arrays.asList(args));

    execute(rootDir, new AssertStatusHook(), argsList.toArray(String[]::new));
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
        new AssertStatusHook(), argsList.toArray(a->new String[a]));
  }

  public static int execute(Path rootDir, StatusHook hook, String... args) {
    RootCommand rootCommand = new RootCommand(rootDir, hook);
    int exitCode = rootCommand.getCmd().execute(args) + (hook.isSuccess() ? 0 : 1);
    if (exitCode != 0) {
      fail();
    }
    return exitCode;
  }

  protected static Path getProjectRoot(Path script) {
    Path currentPath = script.toAbsolutePath();
    while (!currentPath.getFileName().toString().equals("sqrl-testing")) {
      currentPath = currentPath.getParent();
    }

    return currentPath.getParent();
  }

  protected static Path getProjectRoot() {
    String userDir = System.getProperty("user.dir");
    Path path = Path.of(userDir);
    return getProjectRoot(path);
  }
}
