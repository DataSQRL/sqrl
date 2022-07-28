package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.sqrl.table.TableWithPK;
import ai.datasqrl.schema.Relationship;
import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlNode;

public class JoinBuilder implements JoinBuild {
    UniqueAliasGenerator aliasGenerator;
    JoinDeclarationContainer joinDeclarations;
    TableMapper tableMapper;
    SqlNodeBuilder sqlBuilder;
    String firstAlias;
    String currentAlias;
    SqlNode currentNode;
    Optional<SqlNode> pullupCondition;

    public JoinBuilder(UniqueAliasGenerator aliasGenerator,
        JoinDeclarationContainer joinDeclarations,
        TableMapper tableMapper, SqlNodeBuilder sqlBuilder) {
      this.aliasGenerator = aliasGenerator;
      this.joinDeclarations = joinDeclarations;
      this.tableMapper = tableMapper;
      this.sqlBuilder = sqlBuilder;
    }

    /**
     * E.g:
     * Orders.entries or o.entries
     *
     * First item is always either a Base Table or an alias.
     */
    public static JoinDeclaration expandPath(TablePath path, boolean forceIncludeBase, JoinBuilderFactory joinBuilderFactory) {
      JoinBuild joinBuilder = joinBuilderFactory.create();
      Optional<String> lastAlias = Optional.of(path.getAlias());
      int i = 0;
      if (forceIncludeBase || !path.isRelative()) {
        joinBuilder.addBaseTable(path, lastAlias.filter(a->path.size() == 0));
      } else if (path.isRelative()) {
        Preconditions.checkState(path.size() > 0);
        Preconditions.checkState(path.getBaseAlias().isPresent());
        joinBuilder.addFirstRel(path.getRelationship(0), path.getBaseAlias().get(),
            lastAlias.filter(a->path.size() == 1));
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

      TableWithPK resolvedTable = tableMapper.getTable(path.getBaseTable());
      String newAlias = lastAlias.orElseGet(() -> aliasGenerator.generate(resolvedTable));
      currentNode = sqlBuilder.createTableNode(resolvedTable, newAlias);
      currentAlias = newAlias;
      firstAlias = currentAlias;

      if (path.isRelative()) {
        Preconditions.checkState(path.getBaseAlias().isPresent());
        pullupCondition = Optional.of(sqlBuilder.createSelfEquality(resolvedTable,
            path.getBaseAlias().get(), newAlias));
      } else {
        pullupCondition = Optional.empty();
      }
    }

    //o.entries.parent
    @Override
    public void addFirstRel(Relationship rel /*entries*/, String baseAlias /*o*/, Optional<String> lastAlias) {
      Preconditions.checkState(currentNode == null);
      //join:
      //  sqlnode: entries e
      //  pullup: _.id = e.id
      JoinDeclaration join = joinDeclarations.getDeclaration(rel);
      JoinDeclaration rewritten = join.rewriteSqlNode(baseAlias /*new self alias*/, lastAlias /*if this is the last alias*/, aliasGenerator);
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
      JoinDeclaration join = joinDeclarations.getDeclaration(rel);
      JoinDeclaration rewritten = join.rewriteSqlNode(currentAlias, lastAlias, aliasGenerator);
      Preconditions.checkState(rewritten.getPullupCondition().isPresent());

      currentNode = sqlBuilder.createJoin(JoinType.INNER, currentNode, rewritten.getJoinTree(),
          rewritten.getPullupCondition().get());
      currentAlias = rewritten.getLastAlias();
    }

    @Override
    public JoinDeclaration build() {
      return new JoinDeclarationImpl(pullupCondition, currentNode, firstAlias, currentAlias);
    }
  }