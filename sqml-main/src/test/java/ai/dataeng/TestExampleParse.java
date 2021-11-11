package ai.dataeng;

import ai.dataeng.sqml.parser.SqmlParser;
import ai.dataeng.sqml.tree.Script;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.sql.SQLOutput;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

/**
 * Tests parsing the examples folder
 */
public class TestExampleParse {
  SqmlParser parser = SqmlParser.newSqmlParser();

//  @Test
  public void testScripts() throws IOException {
    Set<Path> passed = new HashSet<>();
    Set<Path> failed = new HashSet<>();
    Map<Path, String> scripts = discoverScripts(Paths.get("../sqml-examples"));
    for (Map.Entry<Path, String> entry : scripts.entrySet()) {
      try {
        System.out.println(entry.getValue());
        Script script = parser.parse(entry.getValue());
        System.out.println(script);
        passed.add(entry.getKey());
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println(String.format("Could not parse %s", entry.getKey().toString()));
        failed.add(entry.getKey());
      }
    }

    if (!failed.isEmpty()) {
      throw new RuntimeException(String.format(
          "Could not parse script: Passed %s \n Failed: %s",
          passed.stream().map(Path::toString).collect(Collectors.joining("\n")),
          failed.stream().map(Path::toString).collect(Collectors.joining("\n"))));
    }
    if (scripts.isEmpty()) {
      throw new RuntimeException("Could not find scripts");
    }
  }

  private Map<Path, String> discoverScripts(Path path) throws IOException {
    PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:**.sqml");

    return Files.walk(path)
        .filter(Files::isRegularFile)
        .filter(matcher::matches)
        .collect(Collectors.toMap(
            p->p, this::getScriptStr
        ));
  }

  private String getScriptStr(Path path) {
    try {
      return new String(Files.readAllBytes(path));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
