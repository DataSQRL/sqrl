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
import com.datasqrl.error.ErrorPrefix;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SqrlConfigTest {

  private ErrorCollector errors;
  private SqrlConfig config;
  private Path tempFile;

  @BeforeEach
  void setUp() {
    errors = new ErrorCollector(ErrorPrefix.ROOT);
    config = SqrlConfig.createCurrentVersion();
  }

  @AfterEach
  @SneakyThrows
  void tearDown() {
    if (tempFile != null) {
      Files.deleteIfExists(tempFile);
      tempFile = null;
    }
  }

  @Test
  void givenMissingKey_whenGetValue_thenThrowsException() {
    SqrlConfig.Value value = config.asString("nonexistent");

    assertThatThrownBy(() -> value.get())
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("Could not find key");
  }

  @Test
  void givenMissingKeyWithDefault_whenGetValue_thenReturnsDefault() {
    SqrlConfig.Value value = config.asString("nonexistent").withDefault("defaultValue");

    assertThat(value.get()).isEqualTo("defaultValue");
  }

  @Test
  void givenKeys_whenGetOptional_thenReturnsCorrectOptionals() {
    assertThat(config.asString("nonexistent").getOptional()).isEmpty();

    config.setProperty("existing", "value");
    assertThat(config.asString("existing").getOptional()).contains("value");
  }

  @Test
  void givenValidValue_whenValidate_thenPasses() {
    config.setProperty("number", "42");

    SqrlConfig.Value value =
        config.asString("number").validate(s -> s.matches("\\d+"), "Must be numeric");

    assertThat(value.get()).isEqualTo("42");
  }

  @Test
  void givenInvalidValue_whenValidate_thenThrows() {
    config.setProperty("number", "42");

    SqrlConfig.Value invalidValue =
        config.asString("number").validate(s -> s.equals("99"), "Must be 99");

    assertThatThrownBy(() -> invalidValue.get())
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("Must be 99");
  }

  @Test
  void givenTypedValues_whenGetValues_thenReturnsCorrectTypes() {
    config.setProperty("intValue", 123);
    config.setProperty("longValue", 456L);
    config.setProperty("boolValue", true);
    config.setProperty("stringValue", "test");
    config.setProperty("listValue", List.of("a", "b", "c"));

    assertThat(config.asInt("intValue").get()).isEqualTo(123);
    assertThat(config.asLong("longValue").get()).isEqualTo(456L);
    assertThat(config.asBool("boolValue").get()).isTrue();
    assertThat(config.asString("stringValue").get()).isEqualTo("test");
    assertThat(config.asList("listValue", String.class).get()).containsExactly("a", "b", "c");
  }

  @Test
  void givenNestedProperties_whenGetSubConfig_thenReturnsNestedValues() {
    var parentConfig = config.getSubConfig("parent");
    var childConfig = parentConfig.getSubConfig("child");
    childConfig.setProperty("value", "nested");

    var subConfig = config.getSubConfig("parent");
    var deepSubConfig = subConfig.getSubConfig("child");
    assertThat(deepSubConfig.asString("value").get()).isEqualTo("nested");
  }

  @Test
  void givenKeys_whenHasKey_thenReturnsCorrectExistence() {
    assertThat(config.hasKey("nonexistent")).isFalse();

    config.setProperty("existing", "value");
    assertThat(config.hasKey("existing")).isTrue();
  }

  @Test
  void givenSourceConfig_whenCopy_thenCopiesAllProperties() {
    var source = SqrlConfig.createCurrentVersion();
    source.setProperty("key1", "value1");
    source.setProperty("key2", 42);
    source.getSubConfig("nested").setProperty("key", "nestedValue");

    config.copy(source);

    assertThat(config.asString("key1").get()).isEqualTo("value1");
    assertThat(config.asInt("key2").get()).isEqualTo(42);
    assertThat(config.getSubConfig("nested").asString("key").get()).isEqualTo("nestedValue");
  }

  @Test
  void givenObject_whenSetProperties_thenSetsAllObjectProperties() {
    var testObj = new TestObject("testName", 99, true);

    config.getSubConfig("testObj").setProperties(testObj);

    var testObjConfig = config.getSubConfig("testObj");
    assertThat(testObjConfig.asString("name").get()).isEqualTo("testName");
    assertThat(testObjConfig.asInt("number").get()).isEqualTo(99);
    assertThat(testObjConfig.asBool("flag").get()).isTrue();
  }

  @Test
  void givenConfigWithObjectData_whenAllAs_thenReturnsObject() {
    var testObjConfig = config.getSubConfig("testObj");
    testObjConfig.setProperty("name", "testName");
    testObjConfig.setProperty("number", 99);
    testObjConfig.setProperty("flag", true);

    var result = config.getSubConfig("testObj").allAs(TestObject.class).get();

    assertThat(result.name).isEqualTo("testName");
    assertThat(result.number).isEqualTo(99);
    assertThat(result.flag).isTrue();
  }

  @Test
  void givenConfigWithMapData_whenAsMap_thenReturnsMapOfObjects() {
    var mapConfig = config.getSubConfig("map");
    var item1Config = mapConfig.getSubConfig("item1");
    item1Config.setProperty("name", "first");
    item1Config.setProperty("number", 1);
    item1Config.setProperty("flag", true);

    var item2Config = mapConfig.getSubConfig("item2");
    item2Config.setProperty("name", "second");
    item2Config.setProperty("number", 2);
    item2Config.setProperty("flag", false);

    var map = config.asMap("map", TestObject.class).get();

    assertThat(map).hasSize(2);
    assertThat(map.get("item1").name).isEqualTo("first");
    assertThat(map.get("item1").number).isEqualTo(1);
    assertThat(map.get("item2").name).isEqualTo("second");
    assertThat(map.get("item2").number).isEqualTo(2);
  }

  @Test
  @SneakyThrows
  void givenConfigWithData_whenToFile_thenWritesAndLoadsCorrectly() {
    config.setProperty("key1", "value1");
    config.setProperty("key2", 42);
    config.getSubConfig("nested").setProperty("key", "nestedValue");

    tempFile = Files.createTempFile("config", ".json");
    config.toFile(tempFile);

    assertThat(tempFile).exists();

    SqrlConfig loadedConfig = SqrlConfigCommons.fromFiles(errors, tempFile);
    assertThat(loadedConfig.asString("key1").get()).isEqualTo("value1");
    assertThat(loadedConfig.asInt("key2").get()).isEqualTo(42);
    assertThat(loadedConfig.getSubConfig("nested").asString("key").get()).isEqualTo("nestedValue");
  }

  @Test
  @SneakyThrows
  void givenConfig_whenToFilePretty_thenWritesFormattedJson() {
    config.setProperty("key1", "value1");
    config.setProperty("key2", 42);

    tempFile = Files.createTempFile("config", ".json");
    config.toFile(tempFile, true);

    assertThat(tempFile).exists();
    String content = Files.readString(tempFile);
    assertThat(content).contains("key1");
    assertThat(content).contains("value1");
  }

  @Test
  void givenConfigWithNestedData_whenToMap_thenReturnsCorrectStructure() {
    config.setProperty("key1", "value1");
    config.setProperty("key2", 42);
    config.getSubConfig("nested").setProperty("key", "nestedValue");

    var map = config.toMap();

    assertThat(map).containsEntry("key1", "value1");
    assertThat(map).containsEntry("key2", 42);
    assertThat(map).containsKey("nested");
    @SuppressWarnings("unchecked")
    var nestedMap = (Map<String, Object>) map.get("nested");
    assertThat(nestedMap).containsEntry("key", "nestedValue");
  }

  @Test
  void givenNewConfig_whenCreateCurrentVersion_thenHasCorrectVersion() {
    var newConfig = SqrlConfig.createCurrentVersion();

    assertThat(newConfig.getVersion()).isEqualTo(1);
  }

  @Test
  void givenNoPaths_whenCreatingConfigFromPackageJson_thenReturnsDefaults() {
    var underTest = SqrlConfigCommons.fromFilesPackageJson(errors, List.of());

    assertThat(underTest).isNotNull();
    assertThat(underTest.getVersion()).isEqualTo(1);
    assertThat(underTest.getEnabledEngines()).contains("vertx", "postgres", "kafka", "flink");
    assertThat(underTest.getTestConfig()).isPresent();
    assertThat(underTest.getEngines().getEngineConfig("flink")).isPresent();
    assertThat(underTest.getScriptConfig().getGraphql()).isEmpty();
    assertThat(underTest.getScriptConfig().getMainScript()).isEmpty();
  }

  @Test
  void givenSinglePath_whenCreatingConfigFromPackageJson_thenOverridesDefaults() {
    var underTest =
        SqrlConfigCommons.fromFilesPackageJson(
            errors, List.of(Path.of("src/test/resources/config/test-package.json")));

    assertThat(underTest).isNotNull();
    assertThat(underTest.getVersion()).isEqualTo(1);

    assertThat(underTest.getEnabledEngines()).contains("test");

    assertThat(underTest.getTestConfig()).isPresent();
    assertThat(underTest.getEngines().getEngineConfig("flink")).isPresent();
    assertThat(underTest.getScriptConfig().getGraphql()).isEmpty();
    assertThat(underTest.getScriptConfig().getMainScript()).isEmpty();
  }

  @Test
  void givenJsonConfigFile_whenLoadViaCommons_thenParsesCorrectly() {
    var config1 =
        SqrlConfigCommons.fromFiles(errors, Path.of("src/test/resources/config/config1.json"));
    testConfig1(config1);
  }

  @Test
  void givenLoadedConfig_whenWriteToFile_thenLoadsIdentically() {
    var loadedConfig =
        SqrlConfigCommons.fromFiles(errors, Path.of("src/test/resources/config/config1.json"));
    var tempFile2 = createTempFile();
    loadedConfig.toFile(tempFile2);
    var reloadedConfig = SqrlConfigCommons.fromFiles(errors, tempFile2);
    testConfig1(reloadedConfig);
  }

  @Test
  void givenSubConfig_whenWriteToFile_thenLoadsCorrectSubset() {
    var loadedConfig =
        SqrlConfigCommons.fromFiles(errors, Path.of("src/test/resources/config/config1.json"));
    var tempFile2 = createTempFile();
    loadedConfig.getSubConfig("subConf").toFile(tempFile2, true);
    var subConfig = SqrlConfigCommons.fromFiles(errors, tempFile2);
    testSubConf(subConfig);
  }

  @Test
  void givenSourceConfig_whenCopy_thenCopiesConfiguration() {
    var other =
        SqrlConfigCommons.fromFiles(errors, Path.of("src/test/resources/config/config1.json"))
            .getSubConfig("subConf");
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
    var tempFile2 = createTempFile();
    newConf.toFile(tempFile2, true);
    var config2 = SqrlConfigCommons.fromFiles(errors, tempFile2);
    assertThat(config2.asBool("test").get()).isTrue();
    var tc2 = config2.getSubConfig("clazz").allAs(TestClass.class).get();
    assertThat(tc2.field1).isEqualTo(tc.field1);
    assertThat(tc2.field2).isEqualTo(tc.field2);
    assertThat(tc2.field3).isEqualTo(tc.field3);
  }

  private void testConfig1(SqrlConfig config) {
    assertThat(config.asInt("key2").get()).isEqualTo(5);
    assertThat(config.asLong("key2").get()).isEqualTo(5L);
    assertThat(config.asString("key1").get()).isEqualTo("value1");
    assertThat(config.asBool("key3").get()).isTrue();
    assertThat(config.asList("list", String.class).get()).isEqualTo(List.of("a", "b", "c"));
    var map = config.asMap("map", TestClass.class).get();
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

  @SneakyThrows
  private Path createTempFile() {
    return Files.createTempFile("configuration", ".json");
  }

  public static class TestObject {
    public String name;
    public int number;
    public boolean flag;

    public TestObject() {}

    public TestObject(String name, int number, boolean flag) {
      this.name = name;
      this.number = number;
      this.flag = flag;
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
