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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import com.datasqrl.error.CollectedException;
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
  void givenJsonConfigFile_whenLoad_thenParsesCorrectly() {
    testConfig1(SqrlConfigCommons.fromFiles(errors, CONFIG_FILE1));
  }

  public void testConfig1(SqrlConfig config) {
    assertThat(config.asInt("key2").get()).isEqualTo(5);
    assertThat(config.asLong("key2").get()).isEqualTo(5L);
    assertThat(config.asString("key1").get()).isEqualTo("value1");
    assertThat(config.asBool("key3").get()).isTrue();
    assertThat(config.asList("list", String.class).get()).isEqualTo(List.of("a", "b", "c"));
    Map<String, TestClass> map = config.asMap("map", TestClass.class).get();
    assertThat(map).hasSize(3);
    assertThat(map.get("e2").field1).isEqualTo(7);
    assertThat(map.get("e3").field2).isEqualTo("flip");
    assertThat(map.get("e1").field3).isEqualTo(List.of("a", "b", "c"));
    assertThat(config.getVersion()).isEqualTo(1);

    var x1 = config.as("x1", ConstraintClass.class).get();
    assertThat(x1.optInt).isEqualTo(2);
    assertThat(x1.flag).isFalse();
    assertThat(x1.optString).isEqualTo("hello world");

    var x2 = config.as("x2", ConstraintClass.class).get();
    assertThat(x2.optInt).isEqualTo(33);

    assertThatThrownBy(() -> config.as("xf1", ConstraintClass.class).get())
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("is not valid");

    assertThatThrownBy(() -> config.as("xf2", ConstraintClass.class).get())
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("Could not find key");

    var nested = config.as("nested", NestedClass.class).get();
    assertThat(nested.counter).isEqualTo(5);
    assertThat(nested.obj.optInt).isEqualTo(33);
    assertThat(nested.obj.flag).isTrue();
  }

  private void testSubConf(SqrlConfig config) {
    assertThat(config.asString("delimited.config.option").get()).isEqualTo("that");
    assertThat(config.asInt("one").get()).isEqualTo(1);
    assertThat(config.asString("token").get()).isEqualTo("piff");
  }

  @Test
  void givenLoadedConfig_whenWriteToFile_thenLoadsIdentically() {
    var config = SqrlConfigCommons.fromFiles(errors, CONFIG_FILE1);
    var tempFile = makeTempFile();
    config.toFile(tempFile);
    var config2 = SqrlConfigCommons.fromFiles(errors, tempFile);
    testConfig1(config2);
  }

  @Test
  void givenSubConfig_whenWriteToFile_thenLoadsCorrectSubset() {
    var config = SqrlConfigCommons.fromFiles(errors, CONFIG_FILE1);
    var tempFile = makeTempFile();
    config.getSubConfig("subConf").toFile(tempFile, true);
    var config2 = SqrlConfigCommons.fromFiles(errors, tempFile);
    testSubConf(config2);
  }

  @Test
  void givenSourceConfig_whenCopy_thenCopiesConfiguration() {
    var other = SqrlConfigCommons.fromFiles(errors, CONFIG_FILE1).getSubConfig("subConf");
    var newConf = SqrlConfig.createCurrentVersion();
    newConf.copy(other);
    testSubConf(newConf);
  }

  @Test
  void givenNewConfig_whenSetPropertiesAndObjects_thenPersistsCorrectly() {
    var newConf = SqrlConfig.createCurrentVersion();
    newConf.setProperty("test", true);
    var tc = new TestClass(9, "boat", List.of("x", "y", "z"));
    newConf.getSubConfig("clazz").setProperties(tc);
    assertThat(newConf.asBool("test").get()).isTrue();
    assertThat(newConf.getSubConfig("clazz").allAs(TestClass.class).get().field3)
        .isEqualTo(tc.field3);
    makeTempFile();
    newConf.toFile(tempFile, true);
    var config2 = SqrlConfigCommons.fromFiles(errors, tempFile);
    assertThat(config2.asBool("test").get()).isTrue();
    var tc2 = config2.getSubConfig("clazz").allAs(TestClass.class).get();
    assertThat(tc2.field1).isEqualTo(tc.field1);
    assertThat(tc2.field2).isEqualTo(tc.field2);
    assertThat(tc2.field3).isEqualTo(tc.field3);
  }

  private Path tempFile;

  @SneakyThrows
  private Path makeTempFile() {
    tempFile = Files.createTempFile(Path.of(""), "configuration", ".json");
    return tempFile;
  }

  @AfterEach
  @SneakyThrows
  void deleteTempFile() {
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
      fail("");
    } catch (Exception e) {
      System.out.println(ErrorPrinter.prettyPrint(errors));
      assertThat(errors.isFatal()).isTrue();
    }
  }
}
