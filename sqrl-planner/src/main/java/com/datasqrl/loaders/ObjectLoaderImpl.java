package com.datasqrl.loaders;


import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.TableConfigLoader;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.config.ExternalDataType;
import com.datasqrl.config.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.module.NamespaceObject;
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
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.flink.table.functions.UserDefinedFunction;

public class ObjectLoaderImpl implements ObjectLoader {

  public static final String FUNCTION_JSON = ".function.json";
  private static final Predicate<String> SCRIPT_IMPORT = Pattern.compile(".*" + FileUtil.toRegex(".sqrl")).asMatchPredicate();
  private final ResourceResolver resourceResolver;
  private final ErrorCollector errors;
  private final CalciteTableFactory tableFactory;
  private final ModuleLoader moduleLoader;
  private final TableConfigLoader tableConfigFactory;

  public ObjectLoaderImpl(ResourceResolver resourceResolver, ErrorCollector errors,
      CalciteTableFactory tableFactory, ModuleLoader moduleLoader,
      TableConfigLoader tableConfigFactory) {
    this.resourceResolver = resourceResolver;
    this.errors = errors;
    this.tableFactory = tableFactory;
    this.moduleLoader = moduleLoader;
    this.tableConfigFactory = tableConfigFactory;
  }

  final static Deserializer SERIALIZER = new Deserializer();

  @Override
  public String toString() {
    return resourceResolver.toString();
  }

  @Override
  public List<NamespaceObject> load(NamePath directory) {
    //Folders take precedence
    List<Path> allItems = resourceResolver.loadPath(directory);

    //Check for sqrl scripts
    if (allItems.isEmpty()) {
      Optional<Path> sqrlFile = resourceResolver.resolveFile(directory.popLast()
          .concat(Name.system(directory.getLast().toString() + ".sqrl")));
      if (sqrlFile.isPresent()) {
        return loadScript(directory, sqrlFile.get());
      }
    }

    return allItems.stream()
            .flatMap(url -> load(url, directory, allItems).stream())
            .collect(Collectors.toList());
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
  private List<NamespaceObject> loadScript(NamePath directory, Path path) {
    return List.of(new ScriptNamespaceObject(moduleLoader, Name.system(""),
        Files.readString(path)));
  }

  @SneakyThrows
  private List<TableNamespaceObject> loadTable(Path path, NamePath basePath, List<Path> allItemsInPath) {
    String tableName = StringUtil.removeFromEnd(ResourceResolver.getFileName(path),DataSource.TABLE_FILE_SUFFIX);
    errors.checkFatal(Name.validName(tableName), "Not a valid table name: %s", tableName);
    TableConfig tableConfig = tableConfigFactory.load(path, Name.system(tableName), errors);

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

    ExternalDataType tableType = tableConfig.getBase().getType();

    if (tableType == ExternalDataType.source ||
        tableType == ExternalDataType.source_and_sink) {
      errors.checkFatal(tableSchema.isPresent(), "Could not find schema file [%s] for table [%s]",
          basePath + "/" + tableConfig.getName().getDisplay(), path);
    }

    switch (tableType) {
      case source:
        return new DataSource()
            .readTableSource(tableSchema.get(), tableConfig, errors, basePath)
            .map(t->new TableSourceNamespaceObject(t, tableFactory, moduleLoader))
            .map(t->(TableNamespaceObject) t)
            .map(List::of)
            .orElse(List.of());
      case sink:
        return new DataSource()
            .readTableSink(tableSchema, tableConfig, basePath)
            .map(TableSinkNamespaceObject::new)
            .map(t->(TableNamespaceObject) t)
            .map(List::of).orElse(List.of());
      case source_and_sink:
        TableSource source = new DataSource().readTableSource(tableSchema.get(), tableConfig, errors, basePath)
            .get();
        TableSink sink = new DataSource().readTableSink(tableSchema, tableConfig, basePath)
            .get();
        return List.of(new TableSourceSinkNamespaceObject(source, sink, tableFactory, moduleLoader));
      default:
        throw new RuntimeException("Unknown table type: "+ tableType);
    }
  }



  public static final Class<?> UDF_FUNCTION_CLASS = UserDefinedFunction.class;

  @SneakyThrows
  private List<NamespaceObject> loadFunction(Path path, NamePath namePath) {
    ObjectNode json = SERIALIZER.mapJsonFile(path, ObjectNode.class);
    String jarPath = json.get("jarPath").asText();
    String functionClassName = json.get("functionClass").asText();

    URL jarUrl = new File(jarPath).toURI().toURL();
    Class<?> functionClass = loadClass(jarPath, functionClassName);
    Preconditions.checkArgument(UDF_FUNCTION_CLASS.isAssignableFrom(functionClass), "Class is not a UserDefinedFunction");

    UserDefinedFunction udf = (UserDefinedFunction) functionClass.getDeclaredConstructor().newInstance();

    // Return a namespace object containing the created function
    return List.of(new FlinkUdfNsObject(FlinkUdfNsObject.getFunctionName(udf), udf, Optional.of(jarUrl)));
  }

  @SneakyThrows
  private Class<?> loadClass(String jarPath, String functionClassName) {
    URL[] urls = {new File(jarPath).toURI().toURL()};
    URLClassLoader classLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
    return Class.forName(functionClassName, true, classLoader);
  }

}
