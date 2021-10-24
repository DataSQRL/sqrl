package ai.dataeng.sqml;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.logical3.LogicalPlan2;
import ai.dataeng.sqml.logical3.LogicalPlan2.LogicalField;
import ai.dataeng.sqml.schema2.ArrayType;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.BooleanType;
import ai.dataeng.sqml.schema2.basic.DateTimeType;
import ai.dataeng.sqml.schema2.basic.FloatType;
import ai.dataeng.sqml.schema2.basic.IntegerType;
import ai.dataeng.sqml.schema2.basic.NullType;
import ai.dataeng.sqml.schema2.basic.NumberType;
import ai.dataeng.sqml.schema2.basic.StringType;
import ai.dataeng.sqml.schema2.basic.UuidType;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.tree.NodeFormatter;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
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

    public Builder analysis(Analysis analysis) {
      this.analysis = analysis;
      return this;
    }

    public GraphQLSchema build() {
      Visitor visitor = new Visitor(analysis, codeRegistryBuilder);
      visitor.visit(analysis.getPlan(), null);
      GraphQLSchema.Builder schemaBuilder = visitor.getBuilder();
      schemaBuilder.codeRegistry(this.codeRegistryBuilder.build());

      analysis.getPhysicalModel().getTables()
              .stream().map(t->t.getQueryAst().map(q->q.accept(new NodeFormatter(), null)))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .forEach(e-> System.out.println(e));

      System.out.println();
      System.out.println(new SchemaPrinter().print(schemaBuilder.build()));

      return schemaBuilder.build();
    }

  }

  static class Visitor extends SqmlTypeVisitor<GraphQLOutputType, Context> {
    private final Analysis analysis;
    private final CodeRegistryBuilder codeRegistryBuilder;
    private GraphQLSchema.Builder schemaBuilder;
    private Map<QualifiedName, GraphQLObjectType.Builder> gqlTypes = new HashMap<>();
    private Set<GraphQLType> additionalTypes = new HashSet<>();
    private GraphQLInputType bind;
    Set<String> seen = new HashSet<>();

    public Visitor(Analysis analysis, CodeRegistryBuilder codeRegistryBuilder) {
      this.analysis = analysis;
      this.codeRegistryBuilder = codeRegistryBuilder;
      this.schemaBuilder = GraphQLSchema.newSchema();
    }

    public GraphQLSchema.Builder getBuilder() {
      return schemaBuilder;
    }

    public GraphQLOutputType visit(LogicalPlan2 logicalPlan, Context context) {
      GraphQLObjectType.Builder obj = GraphQLObjectType.newObject()
          .name("Query");

      for (LogicalField field : logicalPlan.getRoot()) {
        Type type = field.getType();

        String fieldName = toName(field.getName());
        GraphQLFieldDefinition f = GraphQLFieldDefinition.newFieldDefinition()
            .name(fieldName)
            .type(type.accept(this, new Context("Query", fieldName)))
            .build();
        obj.field(f);
      }

      schemaBuilder.query(obj);
      return null;
    }

    @Override
    public GraphQLOutputType visitArrayType(ArrayType type, Context context) {
      return GraphQLList.list(type.getSubType().accept(this, context));
    }

    @Override
    public GraphQLOutputType visitNumberType(NumberType type, Context context) {
      return Scalars.GraphQLFloat;
    }

    @Override
    public GraphQLOutputType visitDateTimeType(DateTimeType type, Context context) {
      return Scalars.GraphQLString;
    }

    @Override
    public GraphQLOutputType visitNullType(NullType type, Context context) {
      return Scalars.GraphQLString;
    }

    @Override
    public GraphQLOutputType visitStringType(StringType type, Context context) {
      return Scalars.GraphQLString;
    }

    @Override
    public GraphQLOutputType visitBooleanType(BooleanType type, Context context) {
      return Scalars.GraphQLBoolean;
    }

    @Override
    public GraphQLOutputType visitFloatType(FloatType type, Context context) {
      return Scalars.GraphQLString;
    }

    @Override
    public GraphQLOutputType visitIntegerType(IntegerType type, Context context) {
      return Scalars.GraphQLString;
    }

    @Override
    public GraphQLOutputType visitUuidType(UuidType type, Context context) {
      return Scalars.GraphQLString;
    }

    @Override
    public <F extends Field> GraphQLOutputType visitRelation(RelationType relationType,
        Context context) {
//            codeRegistryBuilder.buildQuery(context.getParentType(),
//          context.getFieldName(),
//          physicalPlan.getMapper().get(rel));
//
//      String name = toName(relationType.);

      GraphQLObjectType.Builder obj = GraphQLObjectType.newObject()
          .name(context.fieldName);

      for (Field field : (List<Field>)relationType.getFields()) {
        if (field.isHidden()) {
          continue;
        }
        String fieldName = toName(field.getName().getDisplay());
        GraphQLFieldDefinition f = GraphQLFieldDefinition.newFieldDefinition()
            .name(fieldName)
            .type(field.getType().accept(this, new Context(context.fieldName, fieldName)))
            .build();
        obj.field(f);
      }
      schemaBuilder.additionalType(obj.build());

      return new GraphQLTypeReference(context.fieldName);
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

    public static String toName(Name name) {
      return toGraphqlName(name.getDisplay());
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
