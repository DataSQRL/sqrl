package org.apache.calcite.prepare;

import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare.AbstractPreparingTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.SqrlCalciteTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.util.ImmutableBitSet;
import org.apiguardian.api.API;
import org.apiguardian.api.API.Status;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Currently a placeholder.
 *
 * THis is a delegate for the preparing table. Used during validation and planning. We can use this
 * to fine tune hints and conversion to relations.
 */
@AllArgsConstructor
public class SqrlRelOptTable extends AbstractPreparingTable {

  private final RelOptTableImpl relOptTable;

  @Override
  protected RelOptTable extend(Table extendedTable) {
    throw new RuntimeException("Extending column not supported");
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    return relOptTable.equals(obj);
  }

  @Override
  public int hashCode() {
    return relOptTable.hashCode();
  }

  @Override
  public double getRowCount() {
    return relOptTable.getRowCount();
  }

  @Override
  public @Nullable RelOptSchema getRelOptSchema() {
    return relOptTable.getRelOptSchema();
  }

  @Override
  public RelNode toRel(
      ToRelContext context) {
    return relOptTable.toRel(context);
  }

  @Override
  public @Nullable List<RelCollation> getCollationList() {
    return relOptTable.getCollationList();
  }

  @Override
  public @Nullable RelDistribution getDistribution() {
    return relOptTable.getDistribution();
  }

  @Override
  public boolean isKey(ImmutableBitSet columns) {
    return relOptTable.isKey(columns);
  }

  @Override
  public @Nullable List<ImmutableBitSet> getKeys() {
    return relOptTable.getKeys();
  }

  @Override
  public @Nullable List<RelReferentialConstraint> getReferentialConstraints() {
    return relOptTable.getReferentialConstraints();
  }

  @Override
  public RelDataType getRowType() {
    return relOptTable.getRowType();
  }

  @Override
  public boolean supportsModality(SqlModality modality) {
    return relOptTable.supportsModality(modality);
  }

  @Override
  public boolean isTemporal() {
    return relOptTable.isTemporal();
  }

  @Override
  public List<String> getQualifiedName() {
    return relOptTable.getQualifiedName();
  }

  @Override
  public SqlMonotonicity getMonotonicity(String columnName) {
    return relOptTable.getMonotonicity(columnName);
  }

  @Override
  public SqlAccessType getAllowedAccess() {
    return relOptTable.getAllowedAccess();
  }

  @Override
  public boolean columnHasDefaultValue(RelDataType rowType, int ordinal,
      InitializerContext initializerContext) {
    return relOptTable.columnHasDefaultValue(rowType, ordinal, initializerContext);
  }

  @Override
  public List<ColumnStrategy> getColumnStrategies() {
    return relOptTable.getColumnStrategies();
  }

  @Override
  public String toString() {
    return relOptTable.toString();
  }

  @Override
  public <T> @Nullable T unwrap(Class<T> clazz) {
    return relOptTable.unwrap(clazz);
  }

  @Override
  public @Nullable Expression getExpression(
      Class clazz) {
    return relOptTable.getExpression(clazz);
  }
}
