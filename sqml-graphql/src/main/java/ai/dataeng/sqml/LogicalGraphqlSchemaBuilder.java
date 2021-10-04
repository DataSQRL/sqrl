package ai.dataeng.sqml;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.Field;
import ai.dataeng.sqml.logical.RelationIdentifier;
import ai.dataeng.sqml.logical.LogicalPlan;
import ai.dataeng.sqml.logical.LogicalPlanVisitor;
import ai.dataeng.sqml.logical.RelationDefinition;
import ai.dataeng.sqml.physical.PhysicalPlan;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.ArrayType;
import ai.dataeng.sqml.type.BooleanType;
import ai.dataeng.sqml.type.DateTimeType;
import ai.dataeng.sqml.type.NullType;
import ai.dataeng.sqml.type.NumberType;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.StringType;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.UnknownType;
import graphql.Scalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.idl.SchemaPrinter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogicalGraphqlSchemaBuilder {

  public static Builder newGraphqlSchema() {
    return new Builder();
  }

  public static class Builder {
    private Analysis analysis;
    private CodeRegistryBuilder codeRegistryBuilder = new CodeRegistryBuilder();
    private PhysicalPlan physicalPlan;

    public Builder analysis(Analysis analysis) {
      this.analysis = analysis;
      return this;
    }

    public GraphQLSchema build() {
      Visitor visitor = new Visitor(analysis, codeRegistryBuilder, physicalPlan);
      analysis.getLogicalPlan().accept(visitor, null);
      GraphQLSchema.Builder schemaBuilder = visitor.getBuilder();
      schemaBuilder.codeRegistry(this.codeRegistryBuilder.build());

      System.out.println(new SchemaPrinter().print(schemaBuilder.build()));

      return schemaBuilder.build();
    }

    public Builder physicalPlan(PhysicalPlan physicalPlan) {
      this.physicalPlan = physicalPlan;
      return this;
    }
  }

  static class Visitor extends LogicalPlanVisitor<GraphQLOutputType, Context> {
    private final Analysis analysis;
    private final CodeRegistryBuilder codeRegistryBuilder;
    private final PhysicalPlan physicalPlan;
    private GraphQLSchema.Builder schemaBuilder;
    private Map<QualifiedName, GraphQLObjectType.Builder> gqlTypes = new HashMap<>();
    private Set<GraphQLType> additionalTypes = new HashSet<>();
    private GraphQLInputType bind;
    Set<String> seen = new HashSet<>();

    public Visitor(Analysis analysis, CodeRegistryBuilder codeRegistryBuilder,
        PhysicalPlan physicalPlan) {
      this.analysis = analysis;
      this.codeRegistryBuilder = codeRegistryBuilder;
      this.physicalPlan = physicalPlan;
      this.schemaBuilder = GraphQLSchema.newSchema();
    }

    public GraphQLSchema.Builder getBuilder() {
      return schemaBuilder;
    }

    @Override
    public GraphQLOutputType visit(LogicalPlan logicalPlan, Context context) {
      GraphQLObjectType.Builder obj = GraphQLObjectType.newObject()
          .name("Query");

      for (RelationIdentifier relationIdentifier : logicalPlan.getBaseEntities()) {
        RelationDefinition rel = logicalPlan.getCurrentDefinition(relationIdentifier.getName())
            .get();

        String fieldName = toName(relationIdentifier.getName());
        GraphQLFieldDefinition f = GraphQLFieldDefinition.newFieldDefinition()
            .name(fieldName)
            .type(rel.accept(this, new Context("Query", fieldName)))
            .build();
        obj.field(f);
      }

      schemaBuilder.query(obj);
      return null;
    }

    @Override
    public GraphQLOutputType visitRelationDefinition(RelationDefinition rel, Context context) {
      codeRegistryBuilder.buildQuery(context.getParentType(),
          context.getFieldName(),
          physicalPlan.getMapper().get(rel));

      Optional<GraphQLTypeReference> visited;
      if ((visited = getOrObserve(rel.getRelationIdentifier())).isPresent()) {
        return visited.get();
      }

      String name = toName(rel.getRelationIdentifier().getName());

      GraphQLObjectType.Builder obj = GraphQLObjectType.newObject()
          .name(name);

      for (Field field : rel.getFields()) {
        if (field.isHidden()) {
          continue;
        }
        String fieldName = toName(field.getName().get());
        GraphQLFieldDefinition f = GraphQLFieldDefinition.newFieldDefinition()
            .name(fieldName)
            .type(field.getType().accept(this, new Context(name, fieldName)))
            .build();
        obj.field(f);
      }
      schemaBuilder.additionalType(obj.build());

      return GraphQLList.list(new GraphQLTypeReference(name));
    }

    private Optional<GraphQLTypeReference> getOrObserve(RelationIdentifier relationIdentifier) {
      String name = toName(relationIdentifier.getName());
      if (seen.contains(relationIdentifier)) {
        return Optional.of(new GraphQLTypeReference(name));
      }
      seen.add(name);
      return Optional.empty();
    }

    @Override
    public GraphQLOutputType visitSqmlType(Type type, Context context) {
      throw new RuntimeException("Could not resolve SQML type for the graphql schema");
    }

    @Override
    public GraphQLOutputType visitArray(ArrayType type, Context context) {
      return GraphQLList.list(type.getSubType().accept(this, context));
    }

    @Override
    public GraphQLOutputType visitNumber(NumberType type, Context context) {
      return Scalars.GraphQLFloat;
    }

    @Override
    public GraphQLOutputType visitUnknown(UnknownType type, Context context) {
      return Scalars.GraphQLString;
    }

    @Override
    public GraphQLOutputType visitDateTime(DateTimeType type, Context context) {
      return Scalars.GraphQLString;
    }

    @Override
    public GraphQLOutputType visitNull(NullType type, Context context) {
      return Scalars.GraphQLString;
    }

    @Override
    public GraphQLOutputType visitString(StringType type, Context context) {
      return Scalars.GraphQLString;
    }

    @Override
    public GraphQLOutputType visitBoolean(BooleanType type, Context context) {
      return Scalars.GraphQLBoolean;
    }

    @Override
    public GraphQLOutputType visitScalarType(Type type, Context context) {
      throw new RuntimeException(String.format("Unidentified scalar for api gen: %s", type));
    }

    @Override
    public GraphQLOutputType visitRelation(RelationType type, Context context) {
      throw new RuntimeException(String.format("Unidentified relation %s", type.getClass().getName()));
    }

    public static String toGraphqlName(String name) {
      return name.replaceAll("[^A-Za-z0-9_]", "");
    }

    private List<GraphQLArgument> buildRelationArguments() {
      GraphQLInputType bind = getOrCreateBindType();

      GraphQLArgument filter = GraphQLArgument.newArgument().name("filter")
          .type(Scalars.GraphQLString)
          .build();

      GraphQLArgument filterBind = GraphQLArgument.newArgument().name("filterBind")
          .type(bind)
          .build();

      return List.of(filter, filterBind);
    }

    private GraphQLInputType getOrCreateBindType() {
      if (bind == null) {
        this.bind = GraphQLInputObjectType.newInputObject()
            .name("bind")
            .field(GraphQLInputObjectField.newInputObjectField()
                .name("name")
                .type(Scalars.GraphQLString))
            .field(GraphQLInputObjectField.newInputObjectField()
                .name("type")
                .type(Scalars.GraphQLString))
            .field(GraphQLInputObjectField.newInputObjectField()
                .name("intType")
                .type(Scalars.GraphQLInt)
            ).build();
        additionalTypes.add(bind);
      }
      return bind;
    }

    public static String toName(QualifiedName name) {
      return toGraphqlName(String.join("_", name.getParts()));
    }

    public static String toName(String name) {
      return toGraphqlName(name);
    }

    private boolean containsHiddenField(QualifiedName name) {
      for (String part : name.getParts()) {
        if (part.startsWith("_")) {
          return true;
        }
      }
      return false;
    }

  }

  @Value
  static class Context {
    private final String parentType;
    private final String fieldName;
  }
}
