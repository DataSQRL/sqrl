package com.datasqrl;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.cmd.RootCommand;
import com.datasqrl.cmd.StatusHook;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class UseCasesIT {
  private static final Path RESOURCES = Paths.get("src/test/resources/usecases");

  @Test
  public void testBanking() {
    execute("banking", "loan.sqrl", "loan.graphqls");
  }

  @Test
  public void testClickstream() {
    execute("clickstream", "clickstream-teaser.sqrl", "clickstream-teaser.graphqls");
  }

  @Test
  public void testConference() {
    execute("conference", "conference.sqrl", "conference.graphqls");
  }

  @Test
  public void testRepository() {
    execute("repository", "repo.sqrl", "repo-query.graphqls");
  }


  private void execute(String path, String... args) {
    List<String> argsList = new ArrayList<>();
    argsList.add("test");
    argsList.addAll(List.of(args));
    argsList.add("--nolookup");
//    argsList.add("--profile");
//    argsList.add("../../../../../../../profiles/flink-1.18");
    argsList.add("--profile");
    argsList.add("../../../../../../../profiles/flink-1.16");
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

}
