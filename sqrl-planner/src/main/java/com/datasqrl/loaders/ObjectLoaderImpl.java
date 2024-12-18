package com.datasqrl.loaders;


import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.TableConfigLoader;
import com.datasqrl.engine.log.LogManager;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.config.ExternalDataType;
import com.datasqrl.config.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.FlinkTableNamespaceObject.FlinkTable;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.module.TableNamespaceObject;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.util.BaseFileUtil;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.StringUtil;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.apache.flink.table.functions.UserDefinedFunction;

public class ObjectLoaderImpl implements ObjectLoader {

  public static final String FUNCTION_JSON = ".function.json";
  private static final Predicate<String> SCRIPT_IMPORT = Pattern.compile(".*" + FileUtil.toRegex(".sqrl")).asMatchPredicate();
  private final ResourceResolver resourceResolver;
  private final ErrorCollector errors;
  private final CalciteTableFactory tableFactory;
  private final ModuleLoader moduleLoader;
  private final TableConfigLoader tableConfigFactory;
  private final PackageJson sqrlConfig;
  private final LogManager logManager;

  public ObjectLoaderImpl(ResourceResolver resourceResolver, ErrorCollector errors,
      CalciteTableFactory tableFactory, ModuleLoader moduleLoader,
      TableConfigLoader tableConfigFactory, PackageJson sqrlConfig, LogManager logManager) {
    this.resourceResolver = resourceResolver;
    this.errors = errors;
    this.tableFactory = tableFactory;
    this.moduleLoader = moduleLoader;
    this.tableConfigFactory = tableConfigFactory;
    this.sqrlConfig = sqrlConfig;
    this.logManager = logManager;
  }

  final static Deserializer SERIALIZER = Deserializer.INSTANCE;

  @Override
  public String toString() {
    return resourceResolver.toString();
  }

  @Override
  public Optional<SqrlModule> load(NamePath directory) {
    //Folders take precedence
    List<Path> allItems = resourceResolver.loadPath(directory);

    //Check for sqrl scripts
    if (allItems.isEmpty()) {
      Optional<Path> graphqlFile = getFile(directory, Name.system(directory.getLast().toString() + ".graphqls"));
      Optional<Path> sqrlFile = getFile(directory, Name.system(directory.getLast().toString() + ".sqrl"));

      // Note: Graphql files are awkwardly loaded as an exceptional thing, so try to skip it if its there
      if (sqrlFile.isPresent() && graphqlFile.isEmpty()) {
        return Optional.of(loadScript(directory, sqrlFile.get()));
      }
    }

    List<NamespaceObject> items = allItems.stream()
        .flatMap(url -> load(url, directory, allItems).stream())
        .collect(Collectors.toList());
    if (items.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(new SqrlDirectoryModule(items));
  }

  private Optional<Path> getFile(NamePath directory, Name name) {
    return resourceResolver.resolveFile(directory.popLast()
        .concat(name));
  }

  private List<? extends NamespaceObject> load(Path path, NamePath directory, List<Path> allItems) {
   if (path.toString().endsWith(DataSource.TABLE_FILE_SUFFIX)) {
      return loadTable(path, directory, allItems);
    } else if (path.toString().endsWith(FUNCTION_JSON)) {
      return loadFunction(path, directory);
    }
    return List.of();
  }

  @SneakyThrows
  private SqrlModule loadScript(NamePath namePath, Path path) {
    return new ScriptSqrlModule(moduleLoader, Files.readString(path), Optional.empty(),
        sqrlConfig, logManager, namePath);
  }

  @SneakyThrows
  private List<TableNamespaceObject> loadTable(Path path, NamePath basePath, List<Path> allItemsInPath) {
    String tableName = StringUtil.removeFromEnd(ResourceResolver.getFileName(path),DataSource.TABLE_FILE_SUFFIX);
    errors.checkFatal(Name.validName(tableName), "Not a valid table name: %s", tableName);
    String tableSQL = Files.readString(path);

    //Find all files associated with the table, i.e. that start with the table name followed by '.'
    List<Path> tablesFiles = allItemsInPath.stream().filter( file -> {
        String filename = ResourceResolver.getFileName(file);
        if (filename.length() <= tableName.length() + 1) return false;
        return filename.substring(0,tableName.length()).equalsIgnoreCase(tableName)
            && filename.charAt(tableName.length())=='.';
    }).collect(Collectors.toList());

    List<TableSchema> tableSchemas = tablesFiles.stream().flatMap(file -> {
      String extension = ResourceResolver.getFileName(file).substring(tableName.length());
      Optional<TableSchemaFactory> factory = TableSchemaFactory.loadByExtension(extension);
      return factory.map(f -> f.create(BaseFileUtil.readFile(file), Optional.of(file), errors)).stream();
    }).collect(Collectors.toUnmodifiableList());

    errors.checkFatal(tableSchemas.size()<=1, "Found multiple schemas for table %s with configuration %s", tableName, path);
    Optional<TableSchema> tableSchema = tableSchemas.stream().findFirst();

    return List.of(new FlinkTableNamespaceObject(new FlinkTable(Name.system(tableName), tableSQL, path, tableSchema)));
  }



  public static final Class<?> UDF_FUNCTION_CLASS = UserDefinedFunction.class;

  @SneakyThrows
  private List<NamespaceObject> loadFunction(Path path, NamePath namePath) {
    ObjectNode json = SERIALIZER.mapJsonFile(path, ObjectNode.class);
    String jarPath = json.get("jarPath").asText();
    String functionClassName = json.get("functionClass").asText();
    Optional<Path> path1 = resourceResolver.resolveFile(namePath.concat(Name.system(jarPath)));
    URL jarUrl = path1.get().toUri().toURL();
    Class<?> functionClass = loadClass(jarUrl, functionClassName);
    Preconditions.checkArgument(UDF_FUNCTION_CLASS.isAssignableFrom(functionClass), "Class is not a UserDefinedFunction");

    UserDefinedFunction udf = (UserDefinedFunction) functionClass.getDeclaredConstructor().newInstance();

    // Return a namespace object containing the created function
    String functionName = FlinkUdfNsObject.getFunctionName(udf);
    return List.of(new FlinkUdfNsObject(functionName, udf, functionName, Optional.of(jarUrl)));
  }

  @SneakyThrows
  private Class<?> loadClass(URL jarPath, String functionClassName) {
    URL[] urls = {jarPath};
    URLClassLoader classLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
    return Class.forName(functionClassName, true, classLoader);
  }

}
