package com.datasqrl.packager.preprocess;

import static com.datasqrl.packager.LambdaUtil.rethrowCall;

import com.datasqrl.loaders.ClasspathFunctionLoader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/*
 * Reads a jar and creates sqrl manifest entries in the build directory
 */
@Slf4j
public class JarPreprocessor implements Preprocessor {

  public static final ObjectMapper mapper = SqrlObjectMapper.INSTANCE;

  public static final String SERVICES_PATH = "META-INF/services/";
  public static final Set<String> flinkUdfs = ClasspathFunctionLoader.flinkUdfClasses
          .stream().map(Class::getCanonicalName).collect(Collectors.toSet());

  @Override
  public Pattern getPattern() {
    return Pattern.compile(".*\\.jar$");
  }

  @SneakyThrows
  @Override
  public void processFile(Path path, ProcessorContext processorContext, ErrorCollector errors) {
    try (java.util.jar.JarFile file = new java.util.jar.JarFile(path.toFile())) {
      file.stream()
          .filter(this::isValidEntry)
          .filter(entry -> flinkUdfs.contains(getClassName(entry)))
          .forEach(entry ->
              rethrowCall(() -> processJarEntry(entry, file, processorContext, path)));
    } catch (Exception e) {
      log.warn("Could not jar in path:" + path, e);
    }
  }

  /**
   * Gets the class name for the jar entry
   */
  private String getClassName(java.util.jar.JarEntry entry) {
    return entry.getName().substring(entry.getName().lastIndexOf("/") + 1);
  }

  /**
   * Processes a single jar entry
   */
  private Void processJarEntry(JarEntry entry, JarFile file,
      ProcessorContext processorContext, Path path) throws IOException {
    var input = file.getInputStream(entry);
    var classes = IOUtils.readLines(input, Charset.defaultCharset());

    for (String clazz : classes) {
      var obj = mapper.createObjectNode();
      obj.put("language", "java");
      obj.put("functionClass", clazz);
      obj.put("jarPath", path.toFile().getName());

      // Create a file in a temporary directory
      var functionName = clazz.substring(clazz.lastIndexOf('.') + 1);
      var toFile = createTempFile(obj, functionName);
      processorContext.addDependency(path);
      processorContext.addDependency(toFile.toPath());
      processorContext.addLibrary(path);
    }

    return null;
  }
  /**
   * Checks if the jar entry is valid
   */
  private boolean isValidEntry(java.util.jar.JarEntry entry) {
    return entry.getName().startsWith(SERVICES_PATH) && !entry.getName().endsWith("/");
  }

  /**
   * Creates a temporary file and writes the given object to it
   */
  private File createTempFile(ObjectNode obj, String functionName) throws IOException {
    var functionPath = Files.createTempDirectory("fnc")
        .resolve(functionName + ".function.json");

    mapper.writeValue(functionPath.toFile(), obj);
    return functionPath.toFile();
  }
}