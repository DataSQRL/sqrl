package com.datasqrl.loaders;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystem;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.io.tables.TableSchemaFactory.SchemaFactoryContext;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.local.generate.FlinkUdfNsObject;
import com.datasqrl.plan.local.generate.FunctionNamespaceObject;
import com.datasqrl.plan.local.generate.NamespaceObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.table.functions.UserDefinedFunction;

@AllArgsConstructor
public class URLObjectLoaderImpl implements URLObjectLoader {

  ErrorCollector errors;

  @Override
  public Optional<NamespaceObject> load(URL url, ResourceResolver resourceResolver, NamePath basePath) {
    if (url.getFile().endsWith(".table.json")) {
      return loadTable(url, resourceResolver, basePath);
    } else if (url.getFile().endsWith(".function.json")) {
      return loadFunction(url, resourceResolver, basePath);
    } else if (url.getFile().endsWith("datasystem.json")) {
      return loadDataSystem(url, resourceResolver, basePath);
    }

    return Optional.empty();
  }

  private Optional<NamespaceObject> loadManfiest(URL url, ResourceResolver resourceResolver,
      NamePath basePath) {

    return null;
  }

  @SneakyThrows
  private Optional<NamespaceObject> loadDataSystem(URL url, ResourceResolver resourceResolver,
      NamePath basePath) {
    Deserializer deserializer = new Deserializer();
    DataSystemConfig dataSystemConfig = deserializer.mapJsonFile(Path.of(url.toURI()), DataSystemConfig.class);

    DataSystem dataSystem = dataSystemConfig.initialize(errors);
    return Optional.of(new DataSystemNsObject(basePath, dataSystem));
  }

  @SneakyThrows
  private Optional<NamespaceObject> loadTable(URL url, ResourceResolver resourceResolver,
      NamePath basePath) {
    TableConfig tableConfig = new Deserializer().mapJsonFile(Path.of(url.toURI()), TableConfig.class);

    TableSchemaFactory tableSchemaFactory = loadSchemaFactory("com.datasqrl.schema.input.FlexibleTableSchemaFactory");

    URL schemaURL = url.toURI().resolve("schema.yml").toURL();

    Optional<TableSchema> tableSchema = tableSchemaFactory.create(schemaURL, SchemaFactoryContext.builder()
        .canonicalizer(tableConfig.getNameCanonicalizer())
        .errors(errors)
        .resourceResolver(resourceResolver)
        .mapper(new ObjectMapper())
        .resolvedName(tableConfig.getResolvedName())
        .build());

    TableSource table = new DataSource().readTable(TableSource.class, tableSchema.get(), tableConfig, errors, basePath)
        .orElseThrow(()->new RuntimeException("Could not read table"));

    return Optional.of(new TableSourceNamespaceObject(table));
  }
  private static final Class<?> UDF_FUNCTION_CLASS = UserDefinedFunction.class;

  @SneakyThrows
  private Optional<NamespaceObject> loadFunction(URL url, ResourceResolver resourceResolver, NamePath namePath) {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode json = objectMapper.readValue(url, ObjectNode.class);
    String jarPath = json.get("jarPath").asText();
    String functionClassName = json.get("functionClass").asText();

    URL jarUrl = new File(jarPath).toURI().toURL();
    Class<?> functionClass = loadClass(jarPath, functionClassName);
    Preconditions.checkArgument(UDF_FUNCTION_CLASS.isAssignableFrom(functionClass), "Class is not a UserDefinedFunction");

    UserDefinedFunction udf = (UserDefinedFunction) functionClass.getDeclaredConstructor().newInstance();

    // Return a namespace object containing the created function
    return Optional.of(new FlinkUdfNsObject(Name.system(udf.getClass().getSimpleName()), udf, Optional.of(jarUrl)));
  }

  @SneakyThrows
  private Class<?> loadClass(String jarPath, String functionClassName) {
    URL[] urls = {new File(jarPath).toURI().toURL()};
    URLClassLoader classLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
    return Class.forName(functionClassName, true, classLoader);
  }

  @SneakyThrows
  private TableSchemaFactory loadSchemaFactory(String className) {
    try {
      //use reflection to create the className
      Class<?> clazz = Class.forName(className);
      //if not a Loader.class type throw an error
      if (!TableSchemaFactory.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException("The given class name is not of type SchemaFactory");
      }
      //create instance of the class
      return (TableSchemaFactory) clazz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalStateException("Unable to create Loader Instance", e);
    }
  }
}
