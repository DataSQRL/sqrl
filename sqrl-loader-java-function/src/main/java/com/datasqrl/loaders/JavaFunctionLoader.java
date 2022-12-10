/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import com.datasqrl.function.builtin.time.FlinkFnc;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.table.functions.UserDefinedFunction;

/**
 * All jars are loaded on the class path and resolved with java's ServiceLoader.
 */
public class JavaFunctionLoader extends AbstractLoader {

  public static final String FILE_SUFFIX = ".function.json";
  private static final Pattern CONFIG_FILE_PATTERN = Pattern.compile("(.*)\\.function\\.json$");

  @Override
  public boolean isPackage(Path packageBasePath, NamePath fullPath) {
    return AbstractLoader.isPackagePath(packageBasePath,fullPath);
  }

  @Override
  public Optional<String> loadsFile(Path file) {
    Matcher matcher = CONFIG_FILE_PATTERN.matcher(file.getFileName().toString());
    if (matcher.find()) {
      return Optional.of(matcher.group(1));
    }
    return Optional.empty();
  }

  @Override
  public boolean load(LoaderContext ctx, NamePath fullPath, Optional<Name> alias) {
    NamePath basePath = fullPath.subList(0, fullPath.size() - 1);

    Path baseDir = AbstractLoader.namepath2Path(ctx.getPackagePath(), basePath);

    Path path = baseDir.resolve(fullPath.getLast() + FILE_SUFFIX);
    if (!Files.isRegularFile(path)) {
      return false;
    }

    FunctionJson fnc = deserialize.mapJsonFile(path, FunctionJson.class);
    try {
      Class<?> clazz = Class.forName(fnc.classPath);
      ctx.addFunction(
          new FlinkFnc(alias.map(Name::getCanonical).orElse(clazz.getSimpleName()),
              (UserDefinedFunction) clazz.getDeclaredConstructor().newInstance()
          ));
    } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException |
             IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(
          String.format("Could not find or load class name: %s", fnc.classPath), e);
    }

    return true;
  }

}
