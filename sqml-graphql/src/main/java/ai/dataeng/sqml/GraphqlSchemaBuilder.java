package ai.dataeng.sqml;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.dag.Dag;
import ai.dataeng.sqml.tree.Annotation;
import ai.dataeng.sqml.tree.Assign;
import ai.dataeng.sqml.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.tree.Except;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.Intersect;
import ai.dataeng.sqml.tree.JoinSubexpression;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.TraversalJoin;
import ai.dataeng.sqml.tree.Union;
import ai.dataeng.sqml.type.SqmlType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import graphql.Scalars;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNamedOutputType;
import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.GraphQLUnionType;
import graphql.schema.idl.SchemaPrinter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

public class GraphqlSchemaBuilder {

  public static Builder newGraphqlSchema() {
    return new Builder();
  }

  public static class Builder {

    private Dag dag;

    public Builder dag(Dag dag) {
      this.dag = dag;
      return this;
    }

    public GraphQLSchema build(){
      Script script = dag.getOptimizationResult().getScript();
      Analysis analysis = dag.getOptimizationResult().getAnalysis();
      Visitor visitor = new Visitor(analysis);
      script.accept(visitor, null);
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
  }

  static class Visitor extends DefaultTraversalVisitor<Object, Context> {

    private final Analysis analysis;
    private GraphQLSchema.Builder schemaBuilder;
    private Map<QualifiedName, GraphQLObjectType.Builder> gqlTypes = new HashMap<>();
    private Map<SqmlType, GraphQLScalarType> scalarTypes = new HashMap<>();
    private Set<GraphQLOutputType> additionalTypes = new HashSet<>();
    GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();

    public Visitor(Analysis analysis) {
      this.analysis = analysis;
      schemaBuilder = GraphQLSchema.newSchema();
    }

    @Override
    protected Object visitAssign(Assign node, Context context) {
      GraphQLObjectType.Builder parent = getOrCreateObjectPath(prefixOrBase(node.getName()));

      GraphQLOutputType outputType = (GraphQLOutputType) node.getRhs().accept(this, new Context(node.getName()));
      if (outputType == null) {
        throw new RuntimeException("No graphql type found for:" + node.getName());
      }
      GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
          .name(node.getName().getSuffix())
          .type(outputType)
          .build();
      parent.field(field);

      return null;
    }

    private QualifiedName prefixOrBase(QualifiedName name) {
      return name.getPrefix().isPresent() ? name.getPrefix().get() : name;
    }

    //Todo clean up some
    private GraphQLObjectType.Builder getOrCreateObjectPath(QualifiedName name) {
      GraphQLObjectType.Builder builder = gqlTypes.get(name);
      if (builder != null) {
        return builder;
      }

      String objName = toName(name.getParts());

      GraphQLObjectType.Builder obj = GraphQLObjectType.newObject()
          .name(objName);
      gqlTypes.put(name, obj);

      if (name.getParts().size() == 1) {
        return obj;
      }

      QualifiedName prefixName = QualifiedName.of(name.getParts().subList(0, name.getParts().size() - 1));
      GraphQLObjectType.Builder p = getOrCreateObjectPath(prefixName);

      p.field(GraphQLFieldDefinition.newFieldDefinition()
          .name(name.getSuffix())
          .type(GraphQLList.list(GraphQLTypeReference.typeRef(objName)))
          .build());

      return p;
    }

    @Override
    public GraphQLOutputType visitExpressionAssignment(ExpressionAssignment expressionAssignment,
        Context context) {
      if (expressionAssignment.getExpression() instanceof JoinSubexpression) {
        //root expression is a join traversal
        return (GraphQLOutputType)expressionAssignment.getExpression().accept(this, context);
      } else {
        ResolvedField field = analysis.getResolvedField(expressionAssignment.getExpression());
        return asScalarType(field.getType(), field.isNonNull());
      }
    }

    @Override
    protected Object visitIntersect(Intersect node, Context context) {
      return node.getRelations().get(0).accept(this, context);
    }

    @Override
    protected Object visitExcept(Except node, Context context) {
      return node.getRelations().get(0).accept(this, context);
    }

    @Override
    public Object visitJoinSubexpression(JoinSubexpression node, Context context) {
      return node.getJoin().accept(this, context);
    }

    @Override
    public Object visitTraversalJoin(TraversalJoin node, Context context) {
      String name = toName(node.getTable().getParts());
      GraphQLTypeReference ref = GraphQLTypeReference.typeRef(name);
      if (node.getLimit().isPresent() && node.getLimit().get() == 1) {
        return ref;
      }
      if (node.getInverse().isPresent()) {
        GraphQLObjectType.Builder parent = getOrCreateObjectPath(node.getTable());
        parent.field(
            GraphQLFieldDefinition.newFieldDefinition()
                .name(toName(node.getInverse().get().getParts()))
                .type(GraphQLList.list(
                    GraphQLTypeReference.typeRef(toName(context.getName().getPrefix().get().getParts()))))
                .build()
        );

      }

      return GraphQLList.list(ref);
    }

    private String toName(List<String> parts) {
      return String.join("_", parts);
    }

    @Override
    public GraphQLOutputType visitQueryAssignment(QueryAssignment queryAssignment, Context context) {
      return (GraphQLOutputType)queryAssignment.getQuery().getQueryBody().accept(this, context);
    }

    @Override
    protected Object visitUnion(Union node, Context context) {
      if (followsUnionRules(node)) {
        List<GraphQLObjectType> unionTypes = new ArrayList<>();
        for (Relation relation : node.getRelations()) {
          GraphQLObjectType obj = (GraphQLObjectType) relation.accept(this, context); //todo add scope?
          unionTypes.add(obj);
        }
        String name = getUnionName(node);
        codeRegistry.typeResolver(name, typeResolutionEnvironment -> null);

        return GraphQLUnionType.newUnionType()
            .name(name)
            .possibleTypes(unionTypes.toArray(new GraphQLObjectType[0]))
            .build();
      } else {
        List<ResolvedField> fields = getCommonFields(node);
        if (fields.size() == 0) {
          //Log error if no fields can be found for union type
          return null;
        }
        GraphQLInterfaceType.Builder interfaceType = GraphQLInterfaceType.newInterface();
        String name = analysis.getName(node);
        interfaceType.name(name);
        codeRegistry.typeResolver(name, typeResolutionEnvironment -> null);

        for (ResolvedField field : fields) {
          interfaceType.field(GraphQLFieldDefinition.newFieldDefinition()
              .name(field.getName())
              .type(asScalarType(field.getType(), field.isNonNull()))
              .build());
        }

        for (Relation relation : node.getRelations()) {
          GraphQLOutputType obj = (GraphQLOutputType) relation.accept(this, context.newContextWithInterface(name)); //todo add scope?
          additionalTypes.add(obj);
        }

        return interfaceType.build();
      }
    }

    private List<ResolvedField> getCommonFields(Union node) {
      Set<ResolvedField> commonFields = new HashSet<>();
      commonFields.addAll(analysis.getFields(node.getRelations().get(0)));

      for (int i = 1; i < node.getRelations().size(); i++) {
        List<ResolvedField> fields = analysis.getFields(node.getRelations().get(i));
        commonFields.retainAll(fields);
      }

      return new ArrayList<>(commonFields);
    }

    private String getUnionName(Union union) {
      for (Annotation annotation : union.getAnnotations()) {
        if (annotation.getName().equals("type_name")) {
          return annotation.getValue();
        }
      }
      return analysis.getName(union);
    }

    private boolean followsUnionRules(Union node) {
      return analysis.followsUnionRules(node);
    }

    @Override
    protected Object visitQuerySpecification(QuerySpecification node, Context context) {
      String name = toName(context.getName().getParts());
      GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name(name);
      for (ResolvedField resolvedField : analysis.getFields(node.getSelect())) {
        builder.field(
            GraphQLFieldDefinition.newFieldDefinition()
                .name(resolvedField.getName())
                .type(asScalarType(resolvedField.getType(), resolvedField.isNonNull()))
                .build());
      }

      if (context.getInterfaceName().isPresent()) {
        builder.withInterface(GraphQLTypeReference.typeRef(context.getInterfaceName().get()));
      }

      if (node.getLimit().isPresent() && node.getLimit().get().equalsIgnoreCase("1")) {
        return builder.build();
      }
      gqlTypes.put(context.getName(), builder);
      return GraphQLList.list(GraphQLTypeReference.typeRef(name));
    }

    private GraphQLOutputType asScalarType(SqmlType type, boolean nonNull) {
      GraphQLOutputType outputType = type.accept(new GqlTypeVisitor(), null);

      return nonNull ? GraphQLNonNull.nonNull(outputType) : outputType;
    }

    public GraphQLSchema.Builder getBuilder() {
      for (Entry<QualifiedName, GraphQLObjectType.Builder> entry : gqlTypes.entrySet()) {
        schemaBuilder.additionalType(entry.getValue().build());
      }

      for (GraphQLOutputType addl : this.additionalTypes) {
        schemaBuilder.additionalType(addl);
      }

      schemaBuilder.query(GraphQLObjectType.newObject().name("Query")
        .field(GraphQLFieldDefinition.newFieldDefinition().name("a").type(Scalars.GraphQLString).build()));

      schemaBuilder.codeRegistry(codeRegistry.build());

      return schemaBuilder;
    }
  }
}
