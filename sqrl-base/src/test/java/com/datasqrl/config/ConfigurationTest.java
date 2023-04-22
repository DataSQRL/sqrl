package com.datasqrl.config;

import static org.junit.jupiter.api.Assertions.*;

import com.datasqrl.error.ErrorCollector;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ConfigurationTest {

  public static final Path CONFIG_DIR = Paths.get("src","test","resources","config");
  public static final Path CONFIG_FILE1 = CONFIG_DIR.resolve("config1.json");

  @Test
  public void testJsonConfiguration() {
    System.out.println(CONFIG_DIR.toAbsolutePath().toString());
    ErrorCollector errors = ErrorCollector.root();
    SqrlConfig config = SqrlConfigCommons.fromFiles(errors, CONFIG_FILE1);
    assertEquals(5, config.asInt("key2").get());
    assertEquals("value1", config.asString("key1").get());
    assertTrue(config.asBool("key3").get());
    assertEquals(List.of("a","b","c"), config.asList("list",String.class).get());
    Map<String,TestClass> map = config.asMap("map",TestClass.class).get();
    assertEquals(3, map.size());
    assertEquals(7, map.get("e2").field1);
    assertEquals("flip", map.get("e3").field2);
    assertEquals(List.of("a","b","c"), map.get("e1").field3);

    ConstraintClass x1 = config.as("x1",ConstraintClass.class).get();
    assertEquals(2, x1.optInt);
    assertFalse(x1.flag);
    assertEquals("hello world", x1.optString);

    ConstraintClass x2 = config.as("x2",ConstraintClass.class).get();
    assertEquals(33, x2.optInt);

    for (int i = 1; i <= 2; i++) {
      try {
        config.as("xf"+i,ConstraintClass.class).get();
        fail();
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }

    NestedClass nested = config.as("nested",NestedClass.class).get();
    assertEquals(5, nested.counter);
    assertEquals(33, nested.obj.optInt);
    assertTrue(nested.obj.flag);
  }

  public static class TestClass {

    int field1;

    @Constraints.MinLength(min = 3)
    String field2;

    List<String> field3;

  }

  public static class ConstraintClass {

    @Constraints.Default
    int optInt = 33;

    @Constraints.MinLength(min = 5) @Constraints.Default
    String optString = "x";

    @Constraints.NotNull
    boolean flag = true;

  }

  public static class NestedClass {

    int counter;

    ConstraintClass obj;

  }

}
