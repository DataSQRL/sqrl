package ai.dataeng.sqml.parser.validator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Multimaps.forMap;
import static com.google.common.collect.Multimaps.unmodifiableMultimap;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.GroupingOperation;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeRef;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Statement;
import ai.dataeng.sqml.tree.Table;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Getter;

@Getter
public class StatementAnalysis {
  @Nullable
  private final Statement root;
  private final List<Expression> parameters;

  private final Map<NodeRef<Node>, Scope> scopes = new LinkedHashMap<>();
  private final Multimap<NodeRef<Expression>, FieldId> columnReferences = ArrayListMultimap.create();

  private final Map<NodeRef<QuerySpecification>, List<Expression>> groupByExpressions = new LinkedHashMap<>();
  private final Map<NodeRef<QuerySpecification>, List<Set<FieldId>>> groupingSets = new LinkedHashMap<>();

  private final Map<NodeRef<Expression>, Type> types = new LinkedHashMap<>();
  private final Map<NodeRef<Expression>, Type> coercions = new LinkedHashMap<>();
  private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();

  private final Map<NodeRef<QuerySpecification>, List<GroupingOperation>> groupingOperations = new LinkedHashMap<>();

  // for recursive view detection
  private final Deque<Table> tablesForView = new ArrayDeque<>();

  public StatementAnalysis(@Nullable Statement root, List<Expression> parameters)
  {
    requireNonNull(parameters);

    this.root = root;
    this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
  }

  public Type getType(Expression expression)
  {
    Type type = types.get(NodeRef.of(expression));
    checkArgument(type != null, "Expression not analyzed: %s", expression);
    return type;
  }

  public boolean isAggregation(QuerySpecification node)
  {
    return groupByExpressions.containsKey(NodeRef.of(node));
  }

  public void setGroupByExpressions(QuerySpecification node, List<Expression> groupingExpressions) {
    groupByExpressions.put(NodeRef.of(node), groupingExpressions);
  }

  public void setGroupingSets(QuerySpecification node, List<Set<FieldId>> set)
  {
    this.groupingSets.put(NodeRef.of(node), set);
  }

  public void addColumnReferences(Map<NodeRef<Expression>, FieldId> columnReferences)
  {
    this.columnReferences.putAll(forMap(columnReferences));
  }

  public void setScope(Node node, Scope scope)
  {
    scopes.put(NodeRef.of(node), scope);
  }

  public Multimap<NodeRef<Expression>, FieldId> getColumnReferenceFields()
  {
    return unmodifiableMultimap(columnReferences);
  }

  public void addTypes(Map<NodeRef<Expression>, Type> types)
  {
    this.types.putAll(types);
  }

  public void addCoercions(Map<NodeRef<Expression>, Type> coercions, Set<NodeRef<Expression>> typeOnlyCoercions)
  {
    this.coercions.putAll(coercions);
    this.typeOnlyCoercions.addAll(typeOnlyCoercions);
  }

  public List<Expression> getParameters()
  {
    return parameters;
  }
}
