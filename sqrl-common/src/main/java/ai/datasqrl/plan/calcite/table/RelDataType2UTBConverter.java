package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.builder.UniversalTableBuilder;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedHashMap;
import java.util.Optional;

@AllArgsConstructor
public class RelDataType2UTBConverter {

    public final UniversalTableBuilder.Factory tableFactory;
    public final NameCanonicalizer canonicalizer;

    public RelDataType2UTBConverter(RelDataTypeFactory typeFactory, int numPrimaryKeys, NameCanonicalizer canonicalizer) {
        this(new TableBuilderFactory(typeFactory, numPrimaryKeys),canonicalizer);
    }

    public UniversalTableBuilder convert(@NonNull NamePath path, RelDataType datatype,
                                         LinkedHashMap<Integer,Name> index2Name) {
        return createBuilder(path, null, datatype, false, index2Name);
    }

    private UniversalTableBuilder createBuilder(@NonNull NamePath path, UniversalTableBuilder parent,
                                                         RelDataType type, boolean isSingleton,
                                                         LinkedHashMap<Integer,Name> index2Name) {
        UniversalTableBuilder tblBuilder;
        if (parent == null) {
            tblBuilder = tableFactory.createTable(path.getLast(),path);
        } else {
            //We assume that the first field is the primary key for the nested table (and don't add an explicit IDX column
            //like we do for imports)
            tblBuilder = tableFactory.createTable(path.getLast(), path, parent, isSingleton);
        }
        //Add fields
        int index = 0;

        for (RelDataTypeField field : type.getFieldList()) {
            boolean isVisible = (index2Name==null);
            Name name = Name.of(field.getName(),canonicalizer);
            if (index2Name!=null && index2Name.containsKey(index)) {
                name = index2Name.get(index);
                isVisible = true;
            }
            Optional<Pair<RelDataType, Relationship.Multiplicity>> nested = getNested(field);
            if (nested.isPresent()) {
                Pair<RelDataType, Relationship.Multiplicity> rel = nested.get();
                UniversalTableBuilder child = createBuilder(path.concat(name),tblBuilder,
                        rel.getKey(), rel.getValue() == Relationship.Multiplicity.MANY, null);
                tblBuilder.addChild(name,child,rel.getValue());
            } else {
                tblBuilder.addColumn(name, field.getType(), isVisible);
            }
            index++;
        }
        return tblBuilder;
    }

    private Optional<Pair<RelDataType, Relationship.Multiplicity>> getNested(RelDataTypeField field) {
        if (CalciteUtil.isNestedTable(field.getType())) {
            Optional<RelDataType> componentType = CalciteUtil.getArrayElementType(field.getType());
            RelDataType nestedType = componentType.orElse(field.getType());
            Relationship.Multiplicity multi = Relationship.Multiplicity.ZERO_ONE;
            if (componentType.isPresent()) multi = Relationship.Multiplicity.MANY;
            else if (!nestedType.isNullable()) multi = Relationship.Multiplicity.ONE;
            return Optional.of(Pair.of(nestedType,multi));
        } else {
            return Optional.empty();
        }

    }

    @Value
    public static class TableBuilderFactory extends UniversalTableBuilder.AbstractFactory {

        int numPrimaryKeys;

        public TableBuilderFactory(RelDataTypeFactory typeFactory, int numPrimaryKeys) {
            super(typeFactory);
            this.numPrimaryKeys = numPrimaryKeys;
        }

        @Override
        public UniversalTableBuilder createTable(@NonNull Name name, @NonNull NamePath path) {
            return new UniversalTableBuilder(name, path, numPrimaryKeys);
        }
    }

}
