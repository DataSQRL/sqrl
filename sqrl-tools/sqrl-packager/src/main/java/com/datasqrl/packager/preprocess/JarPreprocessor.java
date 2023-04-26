package com.datasqrl.packager.preprocess;

import static com.datasqrl.packager.LambdaUtil.rethrowCall;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auto.service.AutoService;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(Preprocessor.class)
/*
 * Reads a jar and creates sqrl manifest entries in the build directory
 */
public class JarPreprocessor implements Preprocessor {

  public static final ObjectMapper mapper = SqrlObjectMapper.INSTANCE;

  public static final String SERVICES_PATH = "META-INF/services/";
  public static final Set<String> flinkUdfs = Set.of(
      ScalarFunction.class.getCanonicalName(),
      AggregateFunction.class.getCanonicalName()
  );

  @Override
  public Pattern getPattern() {
    return Pattern.compile(".*\\.jar$");
  }

  @SneakyThrows
  @Override
  public void loader(Path path, ProcessorContext processorContext, ErrorCollector errors) {
    try (java.util.jar.JarFile file = new java.util.jar.JarFile(path.toFile())) {
      file.stream()
          .filter(this::isValidEntry)
          .filter(entry -> flinkUdfs.contains(getClassName(entry)))
          .forEach(entry ->
              rethrowCall(() -> processJarEntry(entry, file, processorContext, path)));
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
    java.io.InputStream input = file.getInputStream(entry);
    List<String> classes = IOUtils.readLines(input, Charset.defaultCharset());

    for (String clazz : classes) {
      ObjectNode obj = mapper.createObjectNode();
      obj.put("language", "java");
      obj.put("functionClass", clazz);
      obj.put("className", "com.datasqrl.loaders.JavaFunctionLoader");
      obj.put("jarPath", path.toString());

      // Create a file in a temporary directory
      String functionName = clazz.substring(clazz.lastIndexOf('.') + 1);
      File toFile = createTempFile(obj, functionName);
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
    Path functionPath = Files.createTempDirectory("fnc")
        .resolve(functionName + ".function.json");

    mapper.writeValue(functionPath.toFile(), obj);
    return functionPath.toFile();
  }
}