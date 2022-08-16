package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.table.TableWithPK;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.local.generate.Resolve.StatementOp;
import ai.datasqrl.schema.Relationship;
import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlNode;

public class JoinBuilderImpl implements JoinBuilder {

  private final Env env;
  private final StatementOp op;

  String firstAlias;
  String currentAlias;
  SqlNode currentNode;
  Optional<SqlNode> pullupCondition;

  public JoinBuilderImpl(Env env, StatementOp op) {
    this.env = env;
    this.op = op;
  }

  /**
   * E.g: Orders.entries or o.entries
   * <p>
   * First item is always either a Base Table or an alias.
   */
  public static SqlJoinDeclaration expandPath(TablePath path, boolean forceIncludeBase,
      JoinBuilderFactory joinBuilderFactory) {
    JoinBuilder joinBuilder = joinBuilderFactory.create();
    Optional<String> lastAlias = Optional.of(path.getAlias());
    int i = 0;
    if (forceIncludeBase || !path.isRelative()) {
      joinBuilder.addBaseTable(path, lastAlias.filter(a -> path.size() == 0));
    } else if (path.isRelative()) {
      Preconditions.checkState(path.size() > 0);
      Preconditions.checkState(path.getBaseAlias().isPresent());
      joinBuilder.addFirstRel(path.getRelationship(0), path.getBaseAlias().get(),
          lastAlias.filter(a -> path.size() == 1));
      i++;
    }

    for (; i < path.size(); i++) {
      Relationship rel = path.getRelationship(i);
      final int cur = i;
      joinBuilder.append(rel, lastAlias.filter(a -> cur == path.size() - 1));
    }

    return joinBuilder.build();
  }

  @Override
  public void addBaseTable(TablePath path, Optional<String> lastAlias) {
    Preconditions.checkState(currentNode == null);

    TableWithPK resolvedTable = env.getTableMap().get(path.getBaseTable());
    String newAlias = lastAlias.orElseGet(() -> env.getAliasGenerator().generate(resolvedTable));
    currentNode = SqlNodeBuilder.createTableNode(resolvedTable, newAlias);
    currentAlias = newAlias;
    firstAlias = currentAlias;

    if (path.isRelative()) {
      Preconditions.checkState(path.getBaseAlias().isPresent());
      pullupCondition = Optional.of(SqlNodeBuilder.createSelfEquality(resolvedTable,
          path.getBaseAlias().get(), newAlias));
    } else {
      pullupCondition = Optional.empty();
    }
  }

  //o.entries.parent
  @Override
  public void addFirstRel(Relationship rel /*entries*/, String baseAlias /*o*/,
      Optional<String> lastAlias) {
    Preconditions.checkState(currentNode == null);
    //join:
    //  sqlnode: entries e
    //  pullup: _.id = e.id
    SqlJoinDeclaration join = env.getResolvedJoinDeclarations().get(rel);
    SqlJoinDeclaration rewritten = join.rewriteSqlNode(baseAlias /*new self alias*/,
        lastAlias /*if this is the last alias*/, env.getAliasGenerator());
    currentNode = rewritten.getJoinTree();
    currentAlias = rewritten.getLastAlias();
    firstAlias = currentAlias;
    //Pullup condition is always set on the first rel
    pullupCondition = rewritten.getPullupCondition();
  }

  //entries e
  @Override
  public void append(Relationship rel, Optional<String> lastAlias) {
    Preconditions.checkNotNull(currentNode);
    SqlJoinDeclaration join = env.getResolvedJoinDeclarations().get(rel);
    SqlJoinDeclaration rewritten = join.rewriteSqlNode(currentAlias, lastAlias,
        env.getAliasGenerator());
    Preconditions.checkState(rewritten.getPullupCondition().isPresent());

    currentNode = SqlNodeBuilder.createJoin(JoinType.INNER, currentNode, rewritten.getJoinTree(),
        rewritten.getPullupCondition().get());
    currentAlias = rewritten.getLastAlias();
  }

  @Override
  public SqlJoinDeclaration build() {
    return new SqlJoinDeclarationImpl(pullupCondition, currentNode, firstAlias, currentAlias);
  }
}