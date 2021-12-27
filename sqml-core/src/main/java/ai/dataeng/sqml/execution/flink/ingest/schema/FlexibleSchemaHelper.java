package ai.dataeng.sqml.execution.flink.ingest.schema;

import ai.dataeng.sqml.type.constraint.ConstraintHelper;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.SpecialName;

public class FlexibleSchemaHelper {

    public static boolean isNonNull(FlexibleDatasetSchema.FlexibleField field) {
        for (FlexibleDatasetSchema.FieldType type : field.getTypes()) {
            if (!ConstraintHelper.isNonNull(type.getConstraints())) return false;
        }
        return true;
    }

    public static Name getCombinedName(FlexibleDatasetSchema.FlexibleField field,
                                       FlexibleDatasetSchema.FieldType type) {
        Name name = field.getName();
        if (name instanceof SpecialName) {
            if (name.equals(SpecialName.VALUE)) {
                name = Name.system("_value"); //TODO: Need to check if this clashes with other names in RelationType
            } else throw new IllegalArgumentException(String.format("Unrecognized name: %s",name));
        }

        if (!type.getVariantName().equals(SpecialName.SINGLETON)) {
            //TODO Temporarily skip variant naming
//            name = Name.combine(field.getName(),type.getVariantName());
            name = field.getName();
        }
        return name;
    }

}
