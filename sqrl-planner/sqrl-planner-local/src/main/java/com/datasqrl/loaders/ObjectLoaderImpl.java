package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.TableNamespaceObject;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.StringUtil;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.table.functions.UserDefinedFunction;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ObjectLoaderImpl implements ObjectLoader {

  public static final String FUNCTION_JSON = ".function.json";
  ResourceResolver resourceResolver;
  ErrorCollector errors;

  final static Deserializer SERIALIZER = new Deserializer();

  @Override
  public String toString() {
    return resourceResolver.toString();
  }

  @Override
  public List<NamespaceObject> load(NamePath directory) {
    List<URI> allItems = resourceResolver.loadPath(directory);
    return allItems.stream()
            .flatMap(url -> load(url, directory).stream())
            .collect(Collectors.toList());
  }

  @Override
  public ObjectLoaderMetadata getMetadata(NamePath path) {
    return new ObjectLoaderMetadata(
        resourceResolver.loadPath(path),
        resourceResolver.getResourcePath(path),
        List.of(DataSource.DATASYSTEM_FILE, DataSource.TABLE_FILE_SUFFIX, FUNCTION_JSON));
  }

  private List<? extends NamespaceObject> load(URI uri, NamePath directory) {
    if (uri.toString().endsWith(DataSource.DATASYSTEM_FILE)) {
      return loadDataSystem(uri, directory);
    } else if (uri.toString().endsWith(DataSource.TABLE_FILE_SUFFIX)) {
      return loadTable(uri, directory);
    } else if (uri.toString().endsWith(FUNCTION_JSON)) {
      return loadFunction(uri, directory);
    }
    return List.of();
  }

  private List<NamespaceObject> loadDataSystem(URI uri, NamePath basePath) {
    TableConfig discoveryConfig = TableConfig.load(uri, basePath.getLast(), errors);
    DataSystemDiscovery discovery = discoveryConfig.initializeDiscovery();
    return List.of(new DataSystemNsObject(basePath, discovery));
  }

  @SneakyThrows
  private List<TableNamespaceObject> loadTable(URI uri, NamePath basePath) {
    String tableName = StringUtil.removeFromEnd(ResourceResolver.getFileName(uri),DataSource.TABLE_FILE_SUFFIX);
    TableConfig tableConfig = TableConfig.load(uri, Name.system(tableName), errors);
    TableSchemaFactory tableSchemaFactory = tableConfig.getSchemaFactory().orElseThrow(() ->
            errors.exception("Schema has not been configured for table [%s]", uri));

    Optional<TableSchema> tableSchema = tableSchemaFactory.create(basePath, FileUtil.getParent(uri),
        resourceResolver, tableConfig, errors);

    if (tableConfig.getBase().getType() == ExternalDataType.source ||
        tableConfig.getBase().getType() == ExternalDataType.source_and_sink) {
      errors.checkFatal(tableSchema.isPresent(), "Could not find schema file [%s] for table [%s]",
          basePath + "/" + tableConfig.getName().getDisplay(), uri);
    }

    switch (tableConfig.getBase().getType()) {
      case source:
        return new DataSource()
            .readTableSource(tableSchema.get(), tableConfig, errors, basePath)
            .map(TableSourceNamespaceObject::new)
            .map(t->(TableNamespaceObject) t)
            .map(List::of)
            .orElse(List.of());
      case sink:
        return new DataSource()
            .readTableSink(tableSchema, tableConfig, errors, basePath)
            .map(TableSinkNamespaceObject::new)
            .map(t->(TableNamespaceObject) t)
            .map(List::of).orElse(List.of());
      case source_and_sink:
        TableSource source = new DataSource().readTableSource(tableSchema.get(), tableConfig, errors, basePath)
            .get();
        TableSink sink = new DataSource().readTableSink(tableSchema, tableConfig, errors, basePath)
            .get();
        return List.of(new TableSourceSinkNamespaceObject(source, sink));
      default:
        throw new RuntimeException("Unknown table type: "+ tableConfig.getBase().getType());
    }
  }



  private static final Class<?> UDF_FUNCTION_CLASS = UserDefinedFunction.class;

  @SneakyThrows
  private List<NamespaceObject> loadFunction(URI uri, NamePath namePath) {
    ObjectNode json = SERIALIZER.mapJsonFile(uri, ObjectNode.class);
    String jarPath = json.get("jarPath").asText();
    String functionClassName = json.get("functionClass").asText();

    URL jarUrl = new File(jarPath).toURI().toURL();
    Class<?> functionClass = loadClass(jarPath, functionClassName);
    Preconditions.checkArgument(UDF_FUNCTION_CLASS.isAssignableFrom(functionClass), "Class is not a UserDefinedFunction");

    UserDefinedFunction udf = (UserDefinedFunction) functionClass.getDeclaredConstructor().newInstance();

    // Return a namespace object containing the created function
    return List.of(new FlinkUdfNsObject(Name.system(udf.getClass().getSimpleName()), udf, Optional.of(jarUrl)));
  }

  @SneakyThrows
  private Class<?> loadClass(String jarPath, String functionClassName) {
    URL[] urls = {new File(jarPath).toURI().toURL()};
    URLClassLoader classLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
    return Class.forName(functionClassName, true, classLoader);
  }

}
