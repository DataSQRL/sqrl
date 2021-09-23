package ai.dataeng.sqml.ingest.schema.external;

import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.SpecialName;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SchemaExport {

    public SchemaDefinition export(Map<Name, FlexibleDatasetSchema> datasets) {
        SchemaDefinition schema = new SchemaDefinition();
        schema.version = SchemaImport.VERSION.toString();
        schema.datasets = new ArrayList<>(datasets.size());
        datasets.forEach((n,d) -> schema.datasets.add(export(n,d)));
        return schema;
    }

    private DatasetDefinition export(Name name, FlexibleDatasetSchema dataset) {
        DatasetDefinition dd = new DatasetDefinition();
        dd.name = name.getDisplay();
        if (!dataset.getDescription().isEmpty()) dd.description = dataset.getDescription().getDescription();
        dd.tables = new ArrayList<>();
        dataset.forEach(tf -> dd.tables.add(export(tf)));
        return dd;
    }

    private TableDefinition export(FlexibleDatasetSchema.TableField tableField) {
        TableDefinition td = new TableDefinition();
        exportElement(td,tableField);
        td.tests = exportConstraints(tableField.getConstraints());
        td.partial_schema = tableField.isPartialSchema();
        td.columns = exportColumns(tableField.getFields());
        return td;
    }

    private void exportElement(AbstractElementDefinition element, FlexibleDatasetSchema.AbstractField field) {
        element.name = field.getName().getDisplay();
        if (!field.getDescription().isEmpty()) element.description = field.getDescription().getDescription();
        element.default_value = field.getDefault_value();
    }

    private List<String> exportConstraints(List<Constraint> constraints) {
        if (constraints.isEmpty()) return null;
        //TODO: export parameters
        return constraints.stream().map(c -> c.getName().getDisplay()).collect(Collectors.toList());
    }

    private List<FieldDefinition> exportColumns(RelationType<FlexibleDatasetSchema.FlexibleField> relation) {
        return relation.getFields().stream().map(f -> export(f)).collect(Collectors.toList());
    }

    private FieldDefinition export(FlexibleDatasetSchema.FlexibleField field) {
        FieldDefinition fd = new FieldDefinition();
        exportElement(fd,field);
        List<FlexibleDatasetSchema.FieldType> types = field.getTypes();
        if (types.size()==1 && types.get(0).getVariantName().equals(SpecialName.SINGLETON)) {
            FieldTypeDefinitionImpl ftd = export(types.get(0));
            fd.type = ftd.type;
            fd.columns = ftd.columns;
            fd.tests = ftd.tests;
        } else {
            fd.mixed = types.stream().collect(Collectors.toMap(ft -> ft.getVariantName().getDisplay(),ft -> export(ft)));
        }
        return fd;
    }

    private FieldTypeDefinitionImpl export(FlexibleDatasetSchema.FieldType fieldType) {
        FieldTypeDefinitionImpl ftd = new FieldTypeDefinitionImpl();
        Type type = fieldType.getType();
        if (type instanceof BasicType) {
            ftd.type = SchemaImport.BasicTypeParse.export(fieldType);
        } else {
            Preconditions.checkArgument(type instanceof RelationType);
            ftd.columns = exportColumns((RelationType<FlexibleDatasetSchema.FlexibleField>) type);
        }
        ftd.tests = exportConstraints(fieldType.getConstraints());
        return ftd;
    }

}
