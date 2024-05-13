package com.datasqrl;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.cmd.RootCommand;
import com.datasqrl.cmd.StatusHook;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests external use cases that are provided through an environmental variable of the form:
 * rootDir1:command1:packageFilename1|rootDir2:command2:packageFilename2|...
 */
@Slf4j
public class ExternalUseCasesIT {

  public static final Set<String> ALLOWED_COMMANDS = Set.of("compile","test");

  @ParameterizedTest
  @ArgumentsSource(TestCasesProvider.class)
  @Disabled //todo: Must provide at least one test, cannot do that with parameterized tests
  public void testCase(Path rootDir, String command, String packageFilename) {
    List<String> argsList = new ArrayList<>();
    Preconditions.checkArgument(ALLOWED_COMMANDS.contains(command.toLowerCase()),"Unsupported command: %s", command);
    Preconditions.checkArgument(Files.exists(rootDir) && Files.isDirectory(rootDir), "Not a valid root directory: %s", rootDir);
    Path packageFile = rootDir.resolve(packageFilename);
    Preconditions.checkArgument(Files.exists(packageFile), "Not a valid package file: %s", packageFile);
    argsList.add(command);
    argsList.add("-c");
    argsList.add(packageFilename);
    UseCasesIT.execute(rootDir,
        AssertStatusHook.INSTANCE, argsList.toArray(String[]::new));
  }


  public static final String TEST_CASES_DELIMITER = "\\|";
  public static final String ARGUMENTS_DELIMITER = ":";

  public static final String SCRIPT_DELIMITER = "\\+";


  public static final String TEST_CASES_ENV_VARIABLE = "SQRL_EXTERNAL_TEST_CASES";

  static class TestCasesProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {
      String path = System.getenv(TEST_CASES_ENV_VARIABLE);
      Preconditions.checkArgument(!Strings.isNullOrEmpty(path), "Environmental %s variable is empty", TEST_CASES_ENV_VARIABLE);

      String[] testCases = path.split(TEST_CASES_DELIMITER);
      return Arrays.stream(testCases).flatMap(test -> {
        String[] testCase = test.split(ARGUMENTS_DELIMITER);
        Preconditions.checkArgument(testCase.length==3 && Arrays.stream(testCase).noneMatch(Strings::isNullOrEmpty),
            "Not a valid test case: %s", test);
        String[] scripts = testCase[2].split(SCRIPT_DELIMITER);
        return Arrays.stream(scripts).map(s -> Arguments.of(Path.of(testCase[0]), testCase[1], s));
      });
    }
  }


}
