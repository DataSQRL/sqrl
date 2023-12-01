package com.datasqrl.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.datasqrl.FlinkEnvironmentBuilder;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.FlinkSourceFactoryContext;
import com.datasqrl.config.TableDescriptorSourceFactory;
import com.datasqrl.engine.stream.flink.plan.SqrlToFlinkExecutablePlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.KafkaTableSourceFactory;
import com.datasqrl.io.formats.AvroSchemaHolder;
import com.datasqrl.io.formats.AvroTableSchemaFactory;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.plan.table.TableConverter;
import com.datasqrl.schema.converters.UniversalTable2FlinkSchema;
import com.datasqrl.serializer.SerializableSchema;
import com.datasqrl.serializer.SerializableSchema.WaterMarkType;
import com.datasqrl.util.SnapshotTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.api.TableDescriptor;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class AvroSchemaHandlingTest {

  private static Path EXAMPLES_PATH = Path.of("..", "..", "sqrl-examples");
  private static final Path RESOURCE_DIR = Paths.get("src", "test", "resources");


  @ParameterizedTest
  @ArgumentsSource(SchemaProvider.class)
  public void testAvroConversion(Path schemaPath, Path configPath, String name) throws Exception {
    String schemaDef = Files.readString(schemaPath);
    AvroTableSchemaFactory schemaFactory = new AvroTableSchemaFactory();
    assertEquals("avro", schemaFactory.getType());
    ErrorCollector errors = ErrorCollector.root();
    AvroSchemaHolder schema = schemaFactory.create(schemaDef, Optional.of(schemaPath.toUri()), errors);
    assertNotNull(schema.getSchema());
    assertEquals(schemaDef, schema.getSchemaDefinition());
    assertEquals("avro", schema.getSchemaType());

    SnapshotTest.Snapshot snapshot = SnapshotTest.Snapshot.of(getClass(), name);
    Name tblName = Name.system(name);
    AvroSchemaToRelDataTypeFactory converter = new AvroSchemaToRelDataTypeFactory();
    RelDataType type = converter.map(schema, tblName, errors);
    snapshot.addContent(type.getFullTypeString(), "relType");
    assertFalse(errors.hasErrors(), errors.toString());

    TableConverter tblConverter = new TableConverter(TypeFactory.getTypeFactory(), NameCanonicalizer.SYSTEM);
    UniversalTable utb = tblConverter.sourceToTable(schema, true, tblName, errors);

    //Flink Schema
    UniversalTable2FlinkSchema conv1 = new UniversalTable2FlinkSchema();
    snapshot.addContent(conv1.convertSchema(utb).toString(), "flinkSchema");

    SerializableSchema serializableSchema =  SqrlToFlinkExecutablePlan.convertSchema(utb,
        "_source_time", null, WaterMarkType.COLUMN_BY_NAME);
    TableDescriptorSourceFactory sourceFactory = new KafkaTableSourceFactory();
    TableConfig tableConfig = TableConfig.load(configPath, tblName, errors);
    FlinkSourceFactoryContext factoryContext = new FlinkSourceFactoryContext(null, name,
        tableConfig.serialize(),
        tableConfig.getFormat(), UUID.randomUUID());
    TableDescriptor descriptor = FlinkEnvironmentBuilder.getTableDescriptor(sourceFactory,
        factoryContext, serializableSchema);
    snapshot.addContent(descriptor.toString(), "descriptor");


    snapshot.createOrValidate();
  }

  static class SchemaProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {
      return Stream.of(Arguments.of(
          EXAMPLES_PATH.resolve("retail/ecommerce-avro/orders.avsc"),
          EXAMPLES_PATH.resolve("retail/ecommerce-avro/orders.table.json"),
          "orders"),
          Arguments.of(
              EXAMPLES_PATH.resolve("retail/ecommerce-avro/orders.avsc"),
              RESOURCE_DIR.resolve("schema/avro/orders-confluent.table.json"),
              "ordersconfluent"));
    }
  }

}
