package ai.datasqrl.plan.calcite;

import org.apache.calcite.sql.validate.SqlConformance;

public class SqrlConformance implements SqlConformance {

  public static final SqlConformance INSTANCE = new SqrlConformance();

  @Override
  public boolean isLiberal() {
    return false;
  }

  @Override
  public boolean allowCharLiteralAlias() {
    return false;
  }

  @Override
  public boolean isGroupByAlias() {
    return true;
  }

  @Override
  public boolean isGroupByOrdinal() {
    return true;
  }

  @Override
  public boolean isHavingAlias() {
    return true;
  }

  @Override
  public boolean isSortByOrdinal() {
    return true;
  }

  @Override
  public boolean isSortByAlias() {
    return true;
  }

  @Override
  public boolean isSortByAliasObscures() {
    return false;
  }

  @Override
  public boolean isFromRequired() {
    return true;
  }

  @Override
  public boolean splitQuotedTableName() {
    return false;
  }

  @Override
  public boolean allowHyphenInUnquotedTableName() {
    return false;
  }

  @Override
  public boolean isBangEqualAllowed() {
    return false;
  }

  @Override
  public boolean isPercentRemainderAllowed() {
    return false;
  }

  @Override
  public boolean isMinusAllowed() {
    return false;
  }

  @Override
  public boolean isApplyAllowed() {
    return false;
  }

  @Override
  public boolean isInsertSubsetColumnsAllowed() {
    return false;
  }

  @Override
  public boolean allowAliasUnnestItems() {
    return false;
  }

  @Override
  public boolean allowNiladicParentheses() {
    return true; //todo: we probably want this to be false
  }

  @Override
  public boolean allowExplicitRowValueConstructor() {
    return false;
  }

  @Override
  public boolean allowExtend() {
    return false;
  }

  @Override
  public boolean isLimitStartCountAllowed() {
    return false;
  }

  @Override
  public boolean allowGeometry() {
    return false;
  }

  @Override
  public boolean shouldConvertRaggedUnionTypesToVarying() {
    return false;
  }

  @Override
  public boolean allowExtendedTrim() {
    return false;
  }

  @Override
  public boolean allowPluralTimeUnits() {
    return false;
  }

  @Override
  public boolean allowQualifyingCommonColumn() {
    return false;
  }
}
