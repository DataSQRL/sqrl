package ai.dataeng.sqml;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.Field;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.type.BooleanType;
import ai.dataeng.sqml.type.DateTimeType;
import ai.dataeng.sqml.type.FloatType;
import ai.dataeng.sqml.type.IntegerType;
import ai.dataeng.sqml.type.NullType;
import ai.dataeng.sqml.type.NumberType;
import ai.dataeng.sqml.type.RelationType.ImportRelationType;
import ai.dataeng.sqml.type.RelationType.NamedRelationType;
import ai.dataeng.sqml.type.RelationType.RootRelationType;
import ai.dataeng.sqml.type.StringType;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.ArrayType;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import ai.dataeng.sqml.type.UnknownType;
import ai.dataeng.sqml.type.UuidType;
import graphql.Scalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;

public class GraphqlSchemaBuilder {

  public static Builder newGraphqlSchema() {
    return new Builder();
  }

  public static class Builder {

    private Script script;
    private Analysis analysis;

    public Builder script(Script script) {
      this.script = script;
      return this;
    }
    public Builder analysis(Analysis analysis) {
      this.analysis = analysis;
      return this;
    }

    public GraphQLSchema build(){
      Visitor visitor = new Visitor(analysis);
      analysis.getModel().accept(visitor, null);
      GraphQLSchema.Builder schemaBuilder = visitor.getBuilder();
      System.out.println(new SchemaPrinter().print(schemaBuilder.build()));

      return schemaBuilder.build();
    }
  }

  static class Context {

    private final QualifiedName name;
    private final Optional<String> interfaceName;

    public Context(QualifiedName name) {
      this(name, Optional.empty());
    }

    public Context(QualifiedName name, Optional<String> interfaceName) {
      this.name = name;
      this.interfaceName = interfaceName;
    }

    public Optional<String> getInterfaceName() {
      return interfaceName;
    }

    public QualifiedName getName() {
      return name;
    }

    public Context newContextWithInterface(String interfaceName) {
      return new Context(name, Optional.of(interfaceName));
    }

    public Context newContextWithAppendedName(String name) {
      List<String> n = new ArrayList<>();
      n.addAll(this.name.getParts());
      n.add(name);

      return new Context(QualifiedName.of(n), this.interfaceName);
    }
  }

  static class Visitor extends SqmlTypeVisitor<GraphQLOutputType, Context> {

    private Logger log = Logger.getLogger(Visitor.class.getName());
    private final Analysis analysis;
    private GraphQLSchema.Builder schemaBuilder;
    private Map<QualifiedName, GraphQLObjectType.Builder> gqlTypes = new HashMap<>();
    private Set<GraphQLType> additionalTypes = new HashSet<>();
    GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();
    private GraphQLInputType bind;
    Set<String> seen = new HashSet<>();

    public Visitor(Analysis analysis) {
      this.analysis = analysis;
      schemaBuilder = GraphQLSchema.newSchema();
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
      throw new RuntimeException("Unidentified relation");
    }

    @Override
    public GraphQLOutputType visitNamedRelation(NamedRelationType type, Context context) {
      String name = toName(type.getRelationName());
      if (seen.contains(name)) {
        return new GraphQLTypeReference(name);
      }

      seen.add(name);
      GraphQLObjectType.Builder obj = GraphQLObjectType.newObject()
          .name(name);

      for (Field field : type.getFields()) {
        if (field.isHidden()) {
          continue;
        }
        GraphQLFieldDefinition f = GraphQLFieldDefinition.newFieldDefinition()
            .name(toName(field.getName().get()))
            .type(field.getType().accept(this, context))
            .build();
        obj.field(f);
      }
      schemaBuilder.additionalType(obj.build());

      return GraphQLList.list(new GraphQLTypeReference(name));
    }

    @Override
    public GraphQLOutputType visitRootRelation(RootRelationType type, Context context) {
      GraphQLObjectType.Builder obj = GraphQLObjectType.newObject()
          .name("Query");

      for (Field field : type.getVisibleFields()) {
        GraphQLFieldDefinition f = GraphQLFieldDefinition.newFieldDefinition()
            .name(toName(field.getName().get()))
            .type(field.getType().accept(this, context))
            .build();
        obj.field(f);
      }
      schemaBuilder.query(obj);
      return null;
    }

    @Override
    public GraphQLOutputType visitImportRelation(ImportRelationType type, Context context) {
      return visitNamedRelation(type, context);
    }

    @Override
    public GraphQLOutputType visitUuid(UuidType type, Context context) {
      return Scalars.GraphQLString;
    }

    @Override
    public GraphQLOutputType visitFloat(FloatType type, Context context) {
      return Scalars.GraphQLFloat;
    }

    @Override
    public GraphQLOutputType visitInteger(IntegerType type, Context context) {
      return Scalars.GraphQLInt;
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

    public GraphQLSchema.Builder getBuilder() {
      return schemaBuilder;
    }
  }
}
