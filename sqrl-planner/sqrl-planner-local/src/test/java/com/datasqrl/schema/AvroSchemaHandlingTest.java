package com.datasqrl.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.formats.AvroSchemaHolder;
import com.datasqrl.io.formats.AvroTableSchemaFactory;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.plan.table.TableConverter;
import com.datasqrl.schema.converters.UniversalTable2FlinkSchema;
import com.datasqrl.schema.input.FlexibleTableSchemaHolder;
import com.datasqrl.util.SnapshotTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ValueSource;

public class AvroSchemaHandlingTest {

  Path ROOT_PATH = Path.of("..", "..", "sqrl-examples");

  @ParameterizedTest
  @ArgumentsSource(SchemaProvider.class)
  public void testAvroConversion(String schemaFile, String name) throws Exception {
    Path schemaPath = ROOT_PATH.resolve(schemaFile);
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
    UniversalTable2FlinkSchema conv1 = new UniversalTable2FlinkSchema();
    snapshot.addContent(conv1.convertSchema(utb).toString(), "flinkSchema");
    snapshot.createOrValidate();
  }

  static class SchemaProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {
      return Stream.of(Arguments.of("retail/ecommerce-avro/orders.avsc", "orders"));
    }
  }

}
