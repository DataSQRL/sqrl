package ai.datasqrl.schema;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.physical.stream.flink.schema.FlinkTableSchemaGenerator;
import ai.datasqrl.physical.stream.flink.schema.FlinkTypeInfoSchemaGenerator;
import ai.datasqrl.plan.calcite.CalciteSchemaGenerator;
import ai.datasqrl.plan.calcite.sqrl.table.CalciteTableFactory;
import ai.datasqrl.plan.local.BundleTableFactory;
import ai.datasqrl.schema.constraint.Constraint;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import ai.datasqrl.schema.input.InputTableSchema;
import ai.datasqrl.schema.input.RelationType;
import ai.datasqrl.schema.input.external.SchemaDefinition;
import ai.datasqrl.schema.input.external.SchemaImport;
import ai.datasqrl.schema.table.VirtualTableFactory;
import ai.datasqrl.util.TestDataset;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class FlexibleSchemaHandlingTest extends AbstractSQRLIT {

    @SneakyThrows
    public FlexibleDatasetSchema getSchema(TestDataset example) {
        ErrorCollector errors = ErrorCollector.root();
        sourceRegistry.addOrUpdateSource(example.getName(), example.getSource(), errors);
        assertFalse(errors.isFatal(), errors.toString());

        SchemaDefinition schemaDef = SqrlScript.Config.parseSchema(example.getInputSchema().get());
        SchemaImport importer = new SchemaImport(sourceRegistry, Constraint.FACTORY_LOOKUP);
        errors = ErrorCollector.root();
        Map<Name, FlexibleDatasetSchema> schema = importer.convertImportSchema(schemaDef, errors);

        assertFalse(errors.isFatal(),errors.toString());
        assertFalse(schema.isEmpty());
        assertTrue(schema.size()==1);
        FlexibleDatasetSchema dsSchema = schema.get(Name.system(example.getName()));
        assertNotNull(dsSchema);
        return dsSchema;
    }

    @BeforeEach
    public void setup() {
        initialize(IntegrationTestSettings.getInMemory(false));
    }

    @ParameterizedTest
    @ArgumentsSource(SchemaConverterProvider.class)
    public<T, V extends FlexibleTableConverter.Visitor<T>> void conversionTest(TestDataset example, SchemaConverter<T,V> visitorTest) {
        Name tableAlias = Name.system("TestTable");
        FlexibleDatasetSchema schema = getSchema(example);
        for (FlexibleDatasetSchema.TableField table : schema.getFields()) {
            for (boolean hasSourceTimestamp : new boolean[]{true, false}) {
                for (Optional<Name> alias : new Optional[]{Optional.empty(), Optional.of(tableAlias)}) {
                    FlexibleTableConverter converter = new FlexibleTableConverter(new InputTableSchema(table, hasSourceTimestamp), alias);
                    V visitor = visitorTest.visitorSupplier.get();
                    Optional<T> result = converter.apply(visitor);
                    visitorTest.validator.validate(result,alias.orElse(table.getName()),visitor);
                }
            }
        }

    }



    static class WithSchemaProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return TestDataset.generateAsArguments(td -> td.getInputSchema().isPresent());
        }
    }

    static class SchemaConverterProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            List<SchemaConverter> converters = new ArrayList<>();

            //BundleTableFactory
            converters.add(SchemaConverter.of(() -> new BundleTableFactoryTester().getVisitor(),
                    (result, name, btfVisitor)-> {
                        assertTrue(result.isPresent());
                        assertTrue(result.get() instanceof RelationType);
                        BundleTableFactory.TableBuilder tb = btfVisitor.getTableBuilder();
                        assertNotNull(tb);
                        NamePath np = tb.getPath();
                        assertEquals(1, np.getLength());
                        assertEquals(name, np.getLast());
                        assertTrue(tb.fields.size()>0);
                    }));
            //Calcite
            converters.add(SchemaConverter.of(() -> new CalciteSchemaGenerator(new JavaTypeFactoryImpl(),new CalciteTableFactory()),
                    (result, name, calciteVisitor)-> {
                        assertTrue(result.isPresent());
                        RelDataType table = result.get();
                        assertTrue(table.getFieldCount()>0);
                        VirtualTableFactory.TableBuilder tbl = calciteVisitor.getRootTable();
                        assertTrue(tbl.getAllFields().size()>0);
                        System.out.println(table);
                    }));
            //Flink
            converters.add(SchemaConverter.of(() -> new FlinkTypeInfoSchemaGenerator(),
                    (result, name, visitor)-> {
                        assertTrue(result.isPresent());
                        TypeInformation table = result.get();
                        assertTrue(table.isTupleType());
                        System.out.println(table);
                    }));
            converters.add(SchemaConverter.of(() -> new FlinkTableSchemaGenerator(),
                    (result, name, flinkTable)-> {
                        assertFalse(result.isPresent());
                        org.apache.flink.table.api.Schema schema = flinkTable.getSchema();
                        assertTrue(schema.getColumns().size()>0);
                        System.out.println(schema);
                    }));

            return TestDataset.generateAsArguments(td -> td.getInputSchema().isPresent(), converters);
        }
    }

    @Value
    static class SchemaConverter<T, V extends FlexibleTableConverter.Visitor<T>> {
        Supplier<V> visitorSupplier;
        SchemaConverterValidator<T,V> validator;

        static <T, V extends FlexibleTableConverter.Visitor<T>> SchemaConverter<T,V> of(
                                    Supplier<V> visitorSupplier,
                                    SchemaConverterValidator<T,V> validator) {
            return new SchemaConverter<>(visitorSupplier,validator);
        }
    }

    @FunctionalInterface
    static interface SchemaConverterValidator<T, V extends FlexibleTableConverter.Visitor<T>> {

        void validate(Optional<T> result, Name tableName, V visitor);

    }


    static class BundleTableFactoryTester extends BundleTableFactory {

        Visitor getVisitor() {
            return new Visitor();
        }

        class Visitor extends ImportVisitor {

            TableBuilder getTableBuilder() {
                return lastCreatedTable;
            }

        }

    }


}
