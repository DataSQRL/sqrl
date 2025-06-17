/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class ConfigurationTest {

  public static final Path CONFIG_DIR = Path.of("src", "test", "resources", "config");
  public static final Path CONFIG_FILE1 = CONFIG_DIR.resolve("config1.json");

  private ErrorCollector errors = ErrorCollector.root();

  @Test
  public void testJsonConfiguration() {
    //    System.out.println(CONFIG_DIR.toAbsolutePath().toString());
    testConfig1(SqrlConfigCommons.fromFiles(errors, CONFIG_FILE1));
  }

  public void testConfig1(SqrlConfig config) {
    assertEquals(5, config.asInt("key2").get());
    assertEquals(5L, config.asLong("key2").get());
    assertEquals("value1", config.asString("key1").get());
    assertTrue(config.asBool("key3").get());
    assertEquals(List.of("a", "b", "c"), config.asList("list", String.class).get());
    Map<String, TestClass> map = config.asMap("map", TestClass.class).get();
    assertEquals(3, map.size());
    assertEquals(7, map.get("e2").field1);
    assertEquals("flip", map.get("e3").field2);
    assertEquals(List.of("a", "b", "c"), map.get("e1").field3);
    assertEquals(1, config.getVersion());

    var x1 = config.as("x1", ConstraintClass.class).get();
    assertEquals(2, x1.optInt);
    assertFalse(x1.flag);
    assertEquals("hello world", x1.optString);

    var x2 = config.as("x2", ConstraintClass.class).get();
    assertEquals(33, x2.optInt);

    for (var i = 1; i <= 2; i++) {
      try {
        config.as("xf" + i, ConstraintClass.class).get();
        fail();
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }

    var nested = config.as("nested", NestedClass.class).get();
    assertEquals(5, nested.counter);
    assertEquals(33, nested.obj.optInt);
    assertTrue(nested.obj.flag);
  }

  private void testSubConf(SqrlConfig config) {
    assertEquals("that", config.asString("delimited.config.option").get());
    assertEquals(1, config.asInt("one").get());
    assertEquals("piff", config.asString("token").get());
  }

  @Test
  public void testWritingFile() {
    var config = SqrlConfigCommons.fromFiles(errors, CONFIG_FILE1);
    var tempFile = makeTempFile();
    config.toFile(tempFile);
    var config2 = SqrlConfigCommons.fromFiles(errors, tempFile);
    testConfig1(config2);
  }

  @Test
  public void testWritingFile2() {
    var config = SqrlConfigCommons.fromFiles(errors, CONFIG_FILE1);
    var tempFile = makeTempFile();
    config.getSubConfig("subConf").toFile(tempFile, true);
    var config2 = SqrlConfigCommons.fromFiles(errors, tempFile);
    testSubConf(config2);
  }

  @Test
  public void copyTest() {
    var other = SqrlConfigCommons.fromFiles(errors, CONFIG_FILE1).getSubConfig("subConf");
    var newConf = SqrlConfig.createCurrentVersion();
    newConf.copy(other);
    testSubConf(newConf);
  }

  @Test
  public void testCreate() {
    var newConf = SqrlConfig.createCurrentVersion();
    newConf.setProperty("test", true);
    var tc = new TestClass(9, "boat", List.of("x", "y", "z"));
    newConf.getSubConfig("clazz").setProperties(tc);
    assertTrue(newConf.asBool("test").get());
    assertEquals(tc.field3, newConf.getSubConfig("clazz").allAs(TestClass.class).get().field3);
    makeTempFile();
    newConf.toFile(tempFile, true);
    var config2 = SqrlConfigCommons.fromFiles(errors, tempFile);
    assertTrue(config2.asBool("test").get());
    var tc2 = config2.getSubConfig("clazz").allAs(TestClass.class).get();
    assertEquals(tc.field1, tc2.field1);
    assertEquals(tc.field2, tc2.field2);
    assertEquals(tc.field3, tc2.field3);
  }

  private Path tempFile;

  @SneakyThrows
  private Path makeTempFile() {
    tempFile = Files.createTempFile(Path.of(""), "configuration", ".json");
    return tempFile;
  }

  @AfterEach
  @SneakyThrows
  public void deleteTempFile() {
    if (tempFile != null) {
      Files.deleteIfExists(tempFile);
      tempFile = null;
    }
  }

  @AllArgsConstructor
  @NoArgsConstructor
  public static class TestClass {

    int field1;

    @Constraints.MinLength(min = 3)
    String field2;

    List<String> field3;
  }

  public static class ConstraintClass {

    @Constraints.Default int optInt = 33;

    @Constraints.MinLength(min = 5)
    @Constraints.Default
    String optString = "x";

    @Constraints.NotNull boolean flag = true;
  }

  public static class NestedClass {

    int counter;

    ConstraintClass obj;
  }

  public static void testForErrors(Consumer<ErrorCollector> failure) {
    var errors = ErrorCollector.root();
    try {
      failure.accept(errors);
      fail();
    } catch (Exception e) {
      System.out.println(ErrorPrinter.prettyPrint(errors));
      assertTrue(errors.isFatal());
    }
  }
}
