/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.dataeng.sqml.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class QuerySpecification
    extends QueryBody {

  private Select select;
  private Relation from;
  private Optional<Expression> where;
  private Optional<GroupBy> groupBy;
  private Optional<Expression> having;
  private Optional<OrderBy> orderBy;
  private Optional<Limit> limit;
  private List<Integer> parentPrimaryKeys;
  private List<Integer> primaryKeys;

  public QuerySpecification(
      NodeLocation location,
      Select select,
      Relation from,
      Optional<Expression> where,
      Optional<GroupBy> groupBy,
      Optional<Expression> having,
      Optional<OrderBy> orderBy,
      Optional<Limit> limit) {
    this(Optional.of(location), select, from, where, groupBy, having, orderBy, limit);
  }

  public QuerySpecification(
      Optional<NodeLocation> location,
      Select select,
      Relation from,
      Optional<Expression> where,
      Optional<GroupBy> groupBy,
      Optional<Expression> having,
      Optional<OrderBy> orderBy,
      Optional<Limit> limit) {
    super(location);
    requireNonNull(select, "select is null");
    requireNonNull(from, "from is null");
    requireNonNull(where, "where is null");
    requireNonNull(groupBy, "groupBy is null");
    requireNonNull(having, "having is null");
    requireNonNull(orderBy, "orderBy is null");
    requireNonNull(limit, "limit is null");

    this.select = select;
    this.from = from;
    this.where = where;
    this.groupBy = groupBy;
    this.having = having;
    this.orderBy = orderBy;
    this.limit = limit;
  }

  public QuerySpecification(QuerySpecification spec, Select select, Relation from,
      Optional<Expression> where, Optional<GroupBy> groupBy, Optional<Expression> having,
      Optional<OrderBy> orderBy, Optional<Limit> limit) {
    this(spec.getLocation(), select, from, where, groupBy, having, orderBy, limit);
    this.parentPrimaryKeys = spec.getParentPrimaryKeys();
  }

  public Select getSelect() {
    return select;
  }

  public Relation getFrom() {
    return from;
  }

  public Optional<Expression> getWhere() {
    return where;
  }

  public Optional<GroupBy> getGroupBy() {
    return groupBy;
  }

  public Optional<Expression> getHaving() {
    return having;
  }

  public Optional<OrderBy> getOrderBy() {
    return orderBy;
  }

  public Optional<Limit> getLimit() {
    return limit;
  }

  public Optional<Long> parseLimit() {
    return getLimit()
        .filter(l->l.getValue().equalsIgnoreCase("ALL"))
        .map(l->Long.parseLong(l.getValue()));
  }

  public void setSelect(Select select) {
    this.select = select;
  }

  public void setFrom(Relation from) {
    this.from = from;
  }

  public void setWhere(Optional<Expression> where) {
    this.where = where;
  }

  public void setGroupBy(Optional<GroupBy> groupBy) {
    this.groupBy = groupBy;
  }

  public void setHaving(Optional<Expression> having) {
    this.having = having;
  }

  public void setOrderBy(Optional<OrderBy> orderBy) {
    this.orderBy = orderBy;
  }

  public void setLimit(Optional<Limit> limit) {
    this.limit = limit;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitQuerySpecification(this, context);
  }

  @Override
  public List<Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    nodes.add(select);
    nodes.add(from);
    where.ifPresent(nodes::add);
    groupBy.ifPresent(nodes::add);
    having.ifPresent(nodes::add);
    orderBy.ifPresent(nodes::add);
    limit.ifPresent(nodes::add);
    return nodes.build();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("select", select)
        .add("from", from)
        .add("where", where.orElse(null))
        .add("groupBy", groupBy)
        .add("having", having.orElse(null))
        .add("orderBy", orderBy)
        .add("limit", limit.orElse(null))
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    QuerySpecification o = (QuerySpecification) obj;
    return Objects.equals(select, o.select) &&
        Objects.equals(from, o.from) &&
        Objects.equals(where, o.where) &&
        Objects.equals(groupBy, o.groupBy) &&
        Objects.equals(having, o.having) &&
        Objects.equals(orderBy, o.orderBy) &&
        Objects.equals(limit, o.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(select, from, where, groupBy, having, orderBy, limit);
  }

  /**
   * Identifies the parent primary keys in the select list
   */
  public List<Integer> getParentPrimaryKeys() {
    return this.parentPrimaryKeys;
  }

  public void setParentPrimaryKeys(List<Integer> parentPrimaryKeys) {
    this.parentPrimaryKeys = parentPrimaryKeys;
  }
  /**
   * Identifies the parent primary keys in the select list
   */
  public List<Integer> getPrimaryKeys() {
    return this.parentPrimaryKeys;
  }

  public void setPrimaryKeys(List<Integer> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }
}
