package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.SnapshotTest.Snapshot;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public class AbstractUseCaseTest extends AbstractAssetSnapshotTest {

  private boolean hasGraphQL = false;

  protected AbstractUseCaseTest(Path usecaseDirectory) {
    super(usecaseDirectory.resolve("deploy-assets"));
  }


  void testUsecase(Path script, Path graphQlFile, Path packageFile) {
    assertTrue(Files.exists(script));
    Path baseDir = script.getParent();
    //Check if GraphQL exists
    Path graphQLFile = baseDir.resolve(FileUtil.separateExtension(script).getKey() + ".graphqls");

    List<String> arguments = new ArrayList<>();
    arguments.add("compile");
    arguments.add(script.getFileName().toString());
    hasGraphQL = graphQlFile!=null;
    if (hasGraphQL) {
      assert Files.exists(graphQLFile);
      arguments.add(graphQLFile.getFileName().toString());
    }
    if (packageFile!=null) {
      assert Files.exists(packageFile);
      arguments.add("-c"); arguments.add(packageFile.getFileName().toString());
    }
//    arguments.add("-t"); arguments.add(deployDir.toString());
    String testname = Stream.of(script, graphQlFile, packageFile)
        .map(AbstractAssetSnapshotTest::getDisplayName)
        .collect(Collectors.joining("-"));
    AssertStatusHook hook = execute(baseDir, arguments);
    snapshot(testname, hook);
  }

  public void snapshot(String testname, AssertStatusHook hook) {
    this.snapshot = Snapshot.of(testname, getClass());
    if (hook.isFailed()) {
      createFailSnapshot(hook.getFailMessage());
    } else {
      createSnapshot();
    }
  }

  @Override
  public Predicate<Path> getBuildDirFilter() {
    return file -> file.getFileName().toString().equalsIgnoreCase("pipeline_explain.txt")
        || (!hasGraphQL && file.getFileName().toString().endsWith(".graphqls"));
  }

  @Override
  public Predicate<Path> getDeployDirFilter() {
    return file -> false;
  }

  @AllArgsConstructor
  public abstract static class SqrlScriptsAndLocalPackages implements ArgumentsProvider {

    private final Path directory;
    private final boolean includeFails;

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      //Look for all package jsons
      return getSQRLScripts(directory, includeFails)
          .sorted(Comparator.comparing(p -> p.toFile().getName()))
          .flatMap(path -> {
        List<Path> pkgFiles = getPackageFiles(path.getParent());
        Collections.sort(pkgFiles, Comparator.comparing(p -> p.toFile().getName()));
        if (pkgFiles.isEmpty()) pkgFiles.add(null);
        List<Path> graphQLFiles = getScriptGraphQLFiles(path);
        Collections.sort(graphQLFiles, Comparator.comparing(p -> p.toFile().getName()));
        if (graphQLFiles.isEmpty()) graphQLFiles.add(null);
        return graphQLFiles.stream().flatMap(gql -> pkgFiles.stream().map(pkg -> Arguments.of(path, gql, pkg)));
      });
    }
  }

  private Path getProjectRoot() {
    Path currentPath = Paths.get(System.getProperty("user.dir"));
    while (!currentPath.getFileName().toString().equals("sqrl-testing")) {
      currentPath = currentPath.getParent();
    }
    return currentPath.getParent();
  }
}
