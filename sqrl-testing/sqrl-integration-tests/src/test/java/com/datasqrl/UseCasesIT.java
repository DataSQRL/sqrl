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
@Disabled
public class UseCasesIT {
  private static final Path RESOURCES = Paths.get("src/test/resources/usecases");

  @Test //Done
  public void testBanking() {
    execute("banking", "loan.sqrl", "loan.graphqls");
  }

  @Test //Done
  public void testClickstream() {
    execute("clickstream", "clickstream-teaser.sqrl", "clickstream-teaser.graphqls");
  }

  @Test //Done
  public void testConference() {
    execute("conference", "conference.sqrl", "conference.graphqls");
  }

  @Test //Done
  public void testRepository() {
    execute("repository", "repo.sqrl", "repo.graphqls");
  }

  @Test
  public void testSensorsMutation() {
    execute("sensors", "sensors-mutation.sqrl", "sensors-mutation.graphqls", "sensors-mutation");
  }

  @Test
  public void testSensorsFull() {
    execute("sensors", "sensors-full.sqrl", null, "sensors-full");
  }

  @Test
  public void testSeedshopExtended() {
    execute("seedshop-tutorial", "seedshop-extended.sqrl", null, "seedshop-extended");
  }


  @Test
  @Disabled
  public void compile() {
    compile("sensors", "sensors-mutation.sqrl", "sensors-mutation.graphqls");
  }

  private void execute(String path, String script, String graphql) {
    execute(path, script, graphql, null);
  }


  private void execute(String path, String script, String graphql, String testSuffix) {
    List<String> argsList = new ArrayList<>();
    argsList.add("test");
    argsList.add(script);
    if (!Strings.isNullOrEmpty(graphql)) argsList.add(graphql);
    if (testSuffix!=null) {
      argsList.add("-s"); argsList.add("snapshots-"+testSuffix);
      argsList.add("--tests"); argsList.add("tests-"+testSuffix);
    }
    argsList.add("--nolookup");
    argsList.add("--profile");
    argsList.add("../../../../../../../profiles/flink-1.16");
    argsList.add("--profile");
    argsList.add("../../../../../../../profiles/flink-1.17");
    execute(RESOURCES.resolve(path),
        AssertStatusHook.INSTANCE, argsList.toArray(String[]::new));
  }

  private void compile(String path, String script, String graphql) {
    List<String> argsList = new ArrayList<>();
    argsList.add("compile");
    argsList.add(script);
    if (!Strings.isNullOrEmpty(graphql)) argsList.add(graphql);
    argsList.add("--nolookup");
//    argsList.add("--profile");
//    argsList.add("../../../../../../../profiles/flink-1.18");
    argsList.add("--profile");
    argsList.add("../../../../../../../profiles/flink-1-16");
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

  @Test
  @Disabled
  public void testCompileScript() {
    execute(Path.of("/Users/matthias/git/data-product-data-connect-cv/src/main/datasqrl"), AssertStatusHook.INSTANCE,
        "compile", "clinical_views.sqrl", "-c", "test_package_clinical_views.json", "--profile", "profile/", "--nolookup");
  }

}
