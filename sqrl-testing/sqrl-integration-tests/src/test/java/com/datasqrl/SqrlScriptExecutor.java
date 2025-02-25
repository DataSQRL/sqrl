package com.datasqrl;

import static com.datasqrl.UseCasesIT.getProjectRoot;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.cmd.RootCommand;
import com.datasqrl.cmd.StatusHook;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class SqrlScriptExecutor {

  private Path rootDir;
  private String goal;
  private String script;
  private String graphql;
  private String testSuffix;
  private String testPath;
  private String packageJsonPath;
  private List<String> additionalArgs;

  public void execute(StatusHook hook) {
    List<String> argsList = new ArrayList<>();
    argsList.add(goal);
    argsList.add(script);
    if (graphql != null && !graphql.isEmpty()) {
      argsList.add(graphql);
    }
    if (goal.equals("test") && testSuffix != null) {
      argsList.add("-s");
      argsList.add("snapshots-" + testSuffix);
    }
    if (goal.equals("test") && testPath != null) {
      argsList.add("--tests");
      argsList.add("tests-" + testSuffix);
    }
    if (additionalArgs != null && !additionalArgs.isEmpty()) {
      argsList.addAll(additionalArgs);
    }
    if (getPackageJsonPath() != null) {
      argsList.addAll(Arrays.asList("-c", getPackageJsonPath()));
    }
    // Execute the command
    RootCommand rootCommand = new RootCommand(rootDir, hook);
    int exitCode =
        rootCommand.getCmd().execute(argsList.toArray(new String[0])) + (hook.isSuccess() ? 0 : 1);
    if (exitCode != 0) {
      fail();
    }
  }
}