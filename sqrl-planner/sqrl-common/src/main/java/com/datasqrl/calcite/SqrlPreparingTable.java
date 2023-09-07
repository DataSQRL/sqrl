package com.datasqrl.calcite;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.ImmutableBitSet;

@AllArgsConstructor
@Getter
public class SqrlPreparingTable extends Prepare.AbstractPreparingTable {
  RelOptSchema relOptSchema;
  List<String> qualifiedName;
  RelDataType relDataType;
  RelOptTable internalTable;

  @Override
  public List<String> getQualifiedName() {
    return internalTable.getQualifiedName();
  }

  @Override
  public RelOptSchema getRelOptSchema() {
    return relOptSchema;
  }

  @Override
  public RelDataType getRowType() {
    return relDataType;
  }

  @Override
  public SqlMonotonicity getMonotonicity(String s) {
    return null;
  }

  @Override
  public SqlAccessType getAllowedAccess() {
    return null;
  }

  @Override
  public boolean supportsModality(SqlModality sqlModality) {
    return false;
  }

  @Override
  public boolean isTemporal() {
    return false;
  }

  @Override
  public double getRowCount() {
    return internalTable.getRowCount();
  }

  @Override
  public RelNode toRel(ToRelContext toRelContext) {
    return internalTable.toRel(toRelContext);
  }

  @Override
  public List<RelCollation> getCollationList() {
    return internalTable.getCollationList();
  }

  @Override
  public RelDistribution getDistribution() {
    return internalTable.getDistribution();
  }

  @Override
  public boolean isKey(ImmutableBitSet immutableBitSet) {
    return internalTable.isKey(immutableBitSet);
  }

  @Override
  public List<ImmutableBitSet> getKeys() {
    return internalTable.getKeys();
  }

  @Override
  public List<RelReferentialConstraint> getReferentialConstraints() {
    return internalTable.getReferentialConstraints();
  }

  @Override
  public Expression getExpression(Class aClass) {
    return internalTable.getExpression(aClass);
  }

  @Override
  protected RelOptTable extend(Table table) {
    return null;
  }

  @Override
  public List<ColumnStrategy> getColumnStrategies() {
    return internalTable.getColumnStrategies();
  }

  @Override
  public <C> C unwrap(Class<C> aClass) {
    if (aClass == SqrlPreparingTable.class) {
      return (C)this;
    }

    return internalTable.unwrap(aClass);
  }

}
