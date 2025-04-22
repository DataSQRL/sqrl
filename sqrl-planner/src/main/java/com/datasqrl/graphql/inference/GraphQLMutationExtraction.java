package com.datasqrl.graphql.inference;

import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getMutationTypeName;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.GraphqlSchemaParser;
import com.datasqrl.graphql.visitor.GraphqlSchemaVisitor;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.queries.APIMutation;
import com.datasqrl.plan.queries.APISource;
import com.google.inject.Inject;

import graphql.language.ObjectTypeDefinition;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(onConstructor_=@Inject)
@Getter
public class GraphQLMutationExtraction {
  private final GraphqlSchemaParser schemaParser;
  private final RelDataTypeFactory typeFactory;
  private final NameCanonicalizer canonicalizer;
  private final ModuleLoader moduleLoader;
  private final ErrorCollector errors;
  private final APIConnectorManager connectorManager;

  public void analyze(APISource apiSource) {
    var registry = schemaParser.parse(apiSource.getSchemaDefinition());
    var mutationType = (ObjectTypeDefinition) registry
        .getType(getMutationTypeName(registry))
        .orElse(null);

    if (mutationType == null) {
      log.trace("No mutations in {}", apiSource);
    } else {
      List<RelDataTypeField> types = GraphqlSchemaVisitor.accept(
          new InputFieldToRelDataType(registry, typeFactory, canonicalizer),
          mutationType, registry);

      var addedFields = appendFields(types);

      for (RelDataTypeField namedType : addedFields) {
        var apiMutation = new APIMutation(Name.system(namedType.getName()), apiSource,
            namedType.getType(), ReservedName.MUTATION_TIME.getDisplay(), ReservedName.MUTATION_PRIMARY_KEY.getDisplay());
        connectorManager.addMutation(apiMutation);
      }
    }
  }

  private List<RelDataTypeField> appendFields(List<RelDataTypeField> types) {
    List<RelDataTypeField> newFields = new ArrayList<>();
    for (RelDataTypeField field : types) {
      var relRecordType = (RelRecordType) field.getType();
      List<RelDataTypeField> fields = new ArrayList<>(relRecordType.getFieldList());
//      fields.add(new RelDataTypeFieldImpl(DEFAULT_EVENT_TIME_NAME, relRecordType.getFieldList().size(),
//          typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3)));
      fields.add(new RelDataTypeFieldImpl("_uuid", relRecordType.getFieldList().size(),
          typeFactory.createSqlType(SqlTypeName.VARCHAR)));

      newFields.add(new RelDataTypeFieldImpl(field.getName(), field.getIndex(),
          new RelRecordType(relRecordType.getStructKind(), fields, relRecordType.isNullable())));
    }
    return newFields;
  }
}
