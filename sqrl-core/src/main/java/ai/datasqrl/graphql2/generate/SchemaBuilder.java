package ai.datasqrl.graphql2.generate;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.SQRLTable;
import graphql.Scalars;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLTypeReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;

public class SchemaBuilder {
  private final  Map<SQRLTable, String> seenSqrlTables = new HashMap<>();
  private final  List<ObjectTypeBuilder> objectTypeBuilderList = new ArrayList<>();

  @Getter
  private final ObjectTypeBuilder query = new ObjectTypeBuilder("Query");

  public ObjectTypeBuilder createObjectType(SQRLTable table) {
    String name = registerTable(table);
    ObjectTypeBuilder builder = new ObjectTypeBuilder(name);
    objectTypeBuilderList.add(builder);
    return builder;
  }

  private String registerTable(SQRLTable table) {
    String maybeName = uniqueName(table.getName().getDisplay());
    seenSqrlTables.putIfAbsent(table, maybeName);
    return seenSqrlTables.get(table);
  }

  public GraphQLTypeReference getTypeReference(SQRLTable table) {
    String name = registerTable(table);
    return new GraphQLTypeReference(name);
  }

  private String uniqueName(String name) {
    if (seenSqrlTables.containsValue(name)) {
      return uniqueName(name + "_");
    }
    return name;
  }

  private GraphQLOutputType getOutputType(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return Scalars.GraphQLBoolean;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return Scalars.GraphQLInt;
      case DECIMAL:
      case FLOAT:
      case REAL:
      case DOUBLE:
        return Scalars.GraphQLFloat;
      case DATE:
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
      case CHAR:
      case VARCHAR:
        return Scalars.GraphQLString;
      case ARRAY:
        return GraphQLList.list(getOutputType(type));
      case STRUCTURED:
      case ROW:
        String name = uniqueName("struct");
        ObjectTypeBuilder objectTypeBuilder = new ObjectTypeBuilder(name);
        objectTypeBuilderList.add(objectTypeBuilder);
        objectTypeBuilder.createStructField(type.getComponentType());
        return new GraphQLTypeReference(name);
      case BINARY:
      case VARBINARY:
      case NULL:
      case ANY:
      case SYMBOL:
      case MULTISET:
      case DISTINCT:
      case MAP:
      case OTHER:
      case CURSOR:
      case COLUMN_LIST:
      case DYNAMIC_STAR:
      case GEOMETRY:
      case SARG:
      default:
        throw new RuntimeException("Unknown graphql schema type");
    }
  }

  public GraphQLSchema build() {
    return GraphQLSchema.newSchema()
        .query(query.build())
        .additionalTypes(
            objectTypeBuilderList.stream()
              .map(ObjectTypeBuilder::build)
              .collect(Collectors.toSet()))
        .build();
  }

  class ObjectTypeBuilder {
    private final String name;
    public Map<String, GraphQLFieldDefinition> fields = new LinkedHashMap<>();

    public ObjectTypeBuilder(String name) {
      this.name = name;
    }

    public void createScalarField(Name name, RelDataType type) {
      if (isHiddenField(name)) {
        return;
      }
      GraphQLFieldDefinition def = GraphQLFieldDefinition.newFieldDefinition()
          .name(name.getDisplay())
          .type(getOutputType(type))
          .build();
      fields.put(name.getDisplay(), def);
    }

    private boolean isHiddenField(Name name) {
      return name.getCanonical().startsWith("_");
    }

    public void createRelationshipField(Name name, SQRLTable toTable, Multiplicity multiplicity) {
      registerTable(toTable);

      GraphQLFieldDefinition def = GraphQLFieldDefinition.newFieldDefinition()
          .name(name.getDisplay())
          .type(getRelationshipType(toTable, multiplicity))
          .build();

      fields.put(name.getDisplay(), def);
    }

    private GraphQLOutputType getRelationshipType(SQRLTable toTable, Multiplicity multiplicity) {
      switch (multiplicity) {
        case ZERO_ONE:
          return getTypeReference(toTable);
        case ONE:
          return GraphQLNonNull.nonNull(getTypeReference(toTable));
        case MANY:
        default:
          return GraphQLList.list(getTypeReference(toTable));
      }
    }

    public void createStructField(RelDataType type) {

    }

    public GraphQLObjectType build() {
      return GraphQLObjectType.newObject()
          .name(name)
          .fields(new ArrayList<>(fields.values()))
          .build();
    }
  }
}
