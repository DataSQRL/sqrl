//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.calcite.sql.validate;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptSchemaWithSampling;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.schema.CustomColumnResolvingTable;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator.Config;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;
import org.apache.flink.calcite.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.calcite.shaded.com.google.common.collect.Iterables;

//SQRL: change F_SUGGESTER to do our shadowing
public class SqlValidatorUtil {
  public static final Suggester EXPR_SUGGESTER = (original, attempt, size) -> {
    return (String)Util.first(original, "EXPR$") + attempt;
  };
  public static final Suggester F_SUGGESTER = (original, attempt, size) -> {
    return (String)(original != null ? original + "$": "$f") + (attempt);
  };
  public static final Suggester ATTEMPT_SUGGESTER = (original, attempt, size) -> {
    return (String)Util.first(original, "$") + attempt;
  };

  private SqlValidatorUtil() {
  }

  public static RelOptTable getRelOptTable(SqlValidatorNamespace namespace, Prepare.CatalogReader catalogReader, String datasetName, boolean[] usedDataset) {
    if (namespace.isWrapperFor(TableNamespace.class)) {
      TableNamespace tableNamespace = (TableNamespace)namespace.unwrap(TableNamespace.class);
      return getRelOptTable(tableNamespace, catalogReader, datasetName, usedDataset, tableNamespace.extendedFields);
    } else {
      if (namespace.isWrapperFor(SqlValidatorImpl.DmlNamespace.class)) {
        SqlValidatorImpl.DmlNamespace dmlNamespace = (SqlValidatorImpl.DmlNamespace)namespace.unwrap(SqlValidatorImpl.DmlNamespace.class);
        SqlValidatorNamespace resolvedNamespace = dmlNamespace.resolve();
        if (resolvedNamespace.isWrapperFor(TableNamespace.class)) {
          TableNamespace tableNamespace = (TableNamespace)resolvedNamespace.unwrap(TableNamespace.class);
          SqlValidatorTable validatorTable = tableNamespace.getTable();
          List<RelDataTypeField> extendedFields = dmlNamespace.extendList == null ? ImmutableList.of() : getExtendedColumns(namespace.getValidator(), validatorTable, dmlNamespace.extendList);
          return getRelOptTable(tableNamespace, catalogReader, datasetName, usedDataset, (List)extendedFields);
        }
      }

      return null;
    }
  }

  private static RelOptTable getRelOptTable(TableNamespace tableNamespace, Prepare.CatalogReader catalogReader, String datasetName, boolean[] usedDataset, List<RelDataTypeField> extendedFields) {
    List<String> names = tableNamespace.getTable().getQualifiedName();
    Object table;
    if (datasetName != null && catalogReader instanceof RelOptSchemaWithSampling) {
      RelOptSchemaWithSampling reader = (RelOptSchemaWithSampling)catalogReader;
      table = reader.getTableForMember(names, datasetName, usedDataset);
    } else {
      table = catalogReader.getTableForMember(names);
    }

    if (!extendedFields.isEmpty()) {
      table = ((RelOptTable)table).extend(extendedFields);
    }

    return (RelOptTable)table;
  }

  public static List<RelDataTypeField> getExtendedColumns(SqlValidator validator, SqlValidatorTable table, SqlNodeList extendedColumns) {
    ImmutableList.Builder<RelDataTypeField> extendedFields = ImmutableList.builder();
    ExtensibleTable extTable = (ExtensibleTable)table.unwrap(ExtensibleTable.class);
    int extendedFieldOffset = extTable == null ? table.getRowType().getFieldCount() : extTable.getExtendedColumnOffset();
    Iterator var6 = pairs(extendedColumns).iterator();

    while(var6.hasNext()) {
      Pair<SqlIdentifier, SqlDataTypeSpec> pair = (Pair)var6.next();
      SqlIdentifier identifier = (SqlIdentifier)pair.left;
      SqlDataTypeSpec type = (SqlDataTypeSpec)pair.right;
      extendedFields.add(new RelDataTypeFieldImpl(identifier.toString(), extendedFieldOffset++, type.deriveType(validator)));
    }

    return extendedFields.build();
  }

  private static List<Pair<SqlIdentifier, SqlDataTypeSpec>> pairs(SqlNodeList extendedColumns) {
    List list = extendedColumns.getList();
    return Util.pairs(list);
  }

  public static ImmutableMap<Integer, RelDataTypeField> getIndexToFieldMap(List<RelDataTypeField> sourceFields, RelDataType targetFields) {
    ImmutableMap.Builder<Integer, RelDataTypeField> output = ImmutableMap.builder();
    Iterator var3 = sourceFields.iterator();

    while(var3.hasNext()) {
      RelDataTypeField source = (RelDataTypeField)var3.next();
      RelDataTypeField target = targetFields.getField(source.getName(), true, false);
      if (target != null) {
        output.put(source.getIndex(), target);
      }
    }

    return output.build();
  }

  public static ImmutableBitSet getOrdinalBitSet(RelDataType sourceRowType, RelDataType targetRowType) {
    Map<Integer, RelDataTypeField> indexToField = getIndexToFieldMap(sourceRowType.getFieldList(), targetRowType);
    return getOrdinalBitSet(sourceRowType, (Map)indexToField);
  }

  public static ImmutableBitSet getOrdinalBitSet(RelDataType sourceRowType, Map<Integer, RelDataTypeField> indexToField) {
    ImmutableBitSet source = ImmutableBitSet.of(Util.transform(sourceRowType.getFieldList(), RelDataTypeField::getIndex));
    ImmutableBitSet target = ImmutableBitSet.of(indexToField.keySet());
    return source.intersect(target);
  }

  public static Map<String, Integer> mapNameToIndex(List<RelDataTypeField> fields) {
    ImmutableMap.Builder<String, Integer> output = ImmutableMap.builder();
    Iterator var2 = fields.iterator();

    while(var2.hasNext()) {
      RelDataTypeField field = (RelDataTypeField)var2.next();
      output.put(field.getName(), field.getIndex());
    }

    return output.build();
  }

  /** @deprecated */
  @Deprecated
  public static RelDataTypeField lookupField(boolean caseSensitive, RelDataType rowType, String columnName) {
    return rowType.getField(columnName, caseSensitive, false);
  }

  public static void checkCharsetAndCollateConsistentIfCharType(RelDataType type) {
    if (SqlTypeUtil.inCharFamily(type)) {
      Charset strCharset = type.getCharset();
      Charset colCharset = type.getCollation().getCharset();

      assert null != strCharset;

      assert null != colCharset;

      if (!strCharset.equals(colCharset)) {
      }
    }

  }

  static void checkIdentifierListForDuplicates(List<SqlNode> columnList, SqlValidatorImpl.ValidationErrorFunction validationErrorFunction) {
    List<List<String>> names = Util.transform(columnList, (o) -> {
      return ((SqlIdentifier)o).names;
    });
    int i = Util.firstDuplicate(names);
    if (i >= 0) {
      throw validationErrorFunction.apply((SqlNode)columnList.get(i), Static.RESOURCE.duplicateNameInColumnList((String)Util.last((List)names.get(i))));
    }
  }

  public static SqlNode addAlias(SqlNode expr, String alias) {
    SqlParserPos pos = expr.getParserPosition();
    SqlIdentifier id = new SqlIdentifier(alias, pos);
    return SqlStdOperatorTable.AS.createCall(pos, new SqlNode[]{expr, id});
  }

  public static String getAlias(SqlNode node, int ordinal) {
    switch (node.getKind()) {
      case AS:
        return ((SqlCall)node).operand(1).toString();
      case OVER:
        return getAlias(((SqlCall)node).operand(0), ordinal);
      case IDENTIFIER:
        return (String)Util.last(((SqlIdentifier)node).names);
      default:
        return ordinal < 0 ? null : SqlUtil.deriveAliasFromOrdinal(ordinal);
    }
  }

  public static SqlValidatorWithHints newValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory, SqlValidator.Config config) {
    return new SqlValidatorImpl(opTab, catalogReader, typeFactory, config);
  }

  /** @deprecated */
  @Deprecated
  public static SqlValidatorWithHints newValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory) {
    return newValidator(opTab, catalogReader, typeFactory, Config.DEFAULT);
  }

  public static String uniquify(String name, Set<String> usedNames, Suggester suggester) {
    if (name != null && usedNames.add(name)) {
      return name;
    } else {
      String originalName = name;
      int j = 0;

      while(true) {
        name = suggester.apply(originalName, j, usedNames.size());
        if (usedNames.add(name)) {
          return name;
        }

        ++j;
      }
    }
  }

  /** @deprecated */
  @Deprecated
  public static List<String> uniquify(List<String> nameList) {
    return uniquify(nameList, EXPR_SUGGESTER, true);
  }

  /** @deprecated */
  @Deprecated
  public static List<String> uniquify(List<String> nameList, Suggester suggester) {
    return uniquify(nameList, suggester, true);
  }

  public static List<String> uniquify(List<String> nameList, boolean caseSensitive) {
    return uniquify(nameList, EXPR_SUGGESTER, caseSensitive);
  }

  public static List<String> uniquify(List<String> nameList, Suggester suggester, boolean caseSensitive) {
    Set<String> used = caseSensitive ? new LinkedHashSet() : new TreeSet(String.CASE_INSENSITIVE_ORDER);
    int changeCount = 0;
    List<String> newNameList = new ArrayList();

    String uniqueName;
    for(Iterator var6 = nameList.iterator(); var6.hasNext(); newNameList.add(uniqueName)) {
      String name = (String)var6.next();
      uniqueName = uniquify(name, (Set)used, suggester);
      if (!uniqueName.equals(name)) {
        ++changeCount;
      }
    }

    return (List)(changeCount == 0 ? nameList : newNameList);
  }

  public static RelDataType deriveJoinRowType(RelDataType leftType, RelDataType rightType, JoinRelType joinType, RelDataTypeFactory typeFactory, List<String> fieldNameList, List<RelDataTypeField> systemFieldList) {
    assert systemFieldList != null;

    switch (joinType) {
      case LEFT:
        rightType = typeFactory.createTypeWithNullability(rightType, true);
        break;
      case RIGHT:
        leftType = typeFactory.createTypeWithNullability(leftType, true);
        break;
      case FULL:
        leftType = typeFactory.createTypeWithNullability(leftType, true);
        rightType = typeFactory.createTypeWithNullability(rightType, true);
        break;
      case SEMI:
      case ANTI:
        rightType = null;
    }

    return createJoinType(typeFactory, leftType, rightType, fieldNameList, systemFieldList);
  }

  public static RelDataType createJoinType(RelDataTypeFactory typeFactory, RelDataType leftType, RelDataType rightType, List<String> fieldNameList, List<RelDataTypeField> systemFieldList) {
    assert fieldNameList == null || fieldNameList.size() == systemFieldList.size() + leftType.getFieldCount() + rightType.getFieldCount();

    List<String> nameList = new ArrayList();
    List<RelDataType> typeList = new ArrayList();
    Set<String> uniqueNameList = typeFactory.getTypeSystem().isSchemaCaseSensitive() ? new HashSet() : new TreeSet(String.CASE_INSENSITIVE_ORDER);
    addFields(systemFieldList, typeList, (List)nameList, (Set)uniqueNameList);
    addFields(leftType.getFieldList(), typeList, (List)nameList, (Set)uniqueNameList);
    if (rightType != null) {
      addFields(rightType.getFieldList(), typeList, (List)nameList, (Set)uniqueNameList);
    }

    if (fieldNameList != null) {
      assert fieldNameList.size() == ((List)nameList).size();

      nameList = fieldNameList;
    }

    return typeFactory.createStructType(typeList, (List)nameList);
  }

  private static void addFields(List<RelDataTypeField> fieldList, List<RelDataType> typeList, List<String> nameList, Set<String> uniqueNames) {
    Iterator var4 = fieldList.iterator();

    while(var4.hasNext()) {
      RelDataTypeField field = (RelDataTypeField)var4.next();
      String name = field.getName();
      if (uniqueNames.contains(name)) {
        String nameBase = name;
        int j = 0;

        while(true) {
          name = nameBase + j;
          if (!uniqueNames.contains(name)) {
            break;
          }

          ++j;
        }
      }

      nameList.add(name);
      uniqueNames.add(name);
      typeList.add(field.getType());
    }

  }

  public static RelDataTypeField getTargetField(RelDataType rowType, RelDataTypeFactory typeFactory, SqlIdentifier id, SqlValidatorCatalogReader catalogReader, RelOptTable table) {
    Table t = table == null ? null : (Table)table.unwrap(Table.class);
    if (!(t instanceof CustomColumnResolvingTable)) {
      SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
      return nameMatcher.field(rowType, id.getSimple());
    } else {
      List<Pair<RelDataTypeField, List<String>>> entries = ((CustomColumnResolvingTable)t).resolveColumn(rowType, typeFactory, id.names);
      switch (entries.size()) {
        case 1:
          if (!((List)((Pair)entries.get(0)).getValue()).isEmpty()) {
            return null;
          }

          return (RelDataTypeField)((Pair)entries.get(0)).getKey();
        default:
          return null;
      }
    }
  }

  public static SqlValidatorNamespace lookup(SqlValidatorScope scope, List<String> names) {
    assert names.size() > 0;

    SqlNameMatcher nameMatcher = scope.getValidator().getCatalogReader().nameMatcher();
    SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
    scope.resolve(ImmutableList.of(names.get(0)), nameMatcher, false, resolved);

    assert resolved.count() == 1;

    SqlValidatorNamespace namespace = resolved.only().namespace;
    Iterator var5 = Util.skip(names).iterator();

    do {
      if (!var5.hasNext()) {
        return namespace;
      }

      String name = (String)var5.next();
      namespace = namespace.lookupChild(name);
    } while(namespace != null);

    throw new AssertionError();
  }

  public static void getSchemaObjectMonikers(SqlValidatorCatalogReader catalogReader, List<String> names, List<SqlMoniker> hints) {
    List<String> subNames = Util.skipLast(names);
    Iterator var4 = catalogReader.getSchemaPaths().iterator();

    while(var4.hasNext()) {
      List<String> x = (List)var4.next();
      List<String> names2 = ImmutableList.<String>builder().addAll(x).addAll(subNames).build();
      hints.addAll(catalogReader.getAllSchemaObjectNames(names2));
    }

  }

  public static SelectScope getEnclosingSelectScope(SqlValidatorScope scope) {
    while(scope instanceof DelegatingScope) {
      if (scope instanceof SelectScope) {
        return (SelectScope)scope;
      }

      scope = ((DelegatingScope)scope).getParent();
    }

    return null;
  }

  public static AggregatingSelectScope getEnclosingAggregateSelectScope(SqlValidatorScope scope) {
    while(scope instanceof DelegatingScope) {
      if (scope instanceof AggregatingSelectScope) {
        return (AggregatingSelectScope)scope;
      }

      scope = ((DelegatingScope)scope).getParent();
    }

    return null;
  }

  public static List<String> deriveNaturalJoinColumnList(SqlNameMatcher nameMatcher, RelDataType leftRowType, RelDataType rightRowType) {
    List<String> naturalColumnNames = new ArrayList();
    List<String> leftNames = leftRowType.getFieldNames();
    List<String> rightNames = rightRowType.getFieldNames();
    Iterator var6 = leftNames.iterator();

    while(var6.hasNext()) {
      String name = (String)var6.next();
      if (nameMatcher.frequency(leftNames, name) == 1 && nameMatcher.frequency(rightNames, name) == 1) {
        naturalColumnNames.add(name);
      }
    }

    return naturalColumnNames;
  }

  public static RelDataType createTypeFromProjection(RelDataType type, List<String> columnNameList, RelDataTypeFactory typeFactory, boolean caseSensitive) {
    List<RelDataTypeField> fields = new ArrayList(columnNameList.size());
    Iterator var5 = columnNameList.iterator();

    while(var5.hasNext()) {
      String name = (String)var5.next();
      RelDataTypeField field = type.getField(name, caseSensitive, false);
      fields.add(type.getFieldList().get(field.getIndex()));
    }

    return typeFactory.createStructType(fields);
  }

  public static void analyzeGroupItem(SqlValidatorScope scope, GroupAnalyzer groupAnalyzer, ImmutableList.Builder<ImmutableList<ImmutableBitSet>> topBuilder, SqlNode groupExpr) {
    switch (groupExpr.getKind()) {
      case ROLLUP:
      case CUBE:
        List<ImmutableBitSet> bitSets = analyzeGroupTuple(scope, groupAnalyzer, ((SqlCall)groupExpr).getOperandList());
        switch (groupExpr.getKind()) {
          case ROLLUP:
            topBuilder.add(rollup(bitSets));
            return;
          default:
            topBuilder.add(cube(bitSets));
            return;
        }
      case OTHER:
        if (groupExpr instanceof SqlNodeList) {
          SqlNodeList list = (SqlNodeList)groupExpr;
          Iterator var7 = list.iterator();

          while(var7.hasNext()) {
            SqlNode node = (SqlNode)var7.next();
            analyzeGroupItem(scope, groupAnalyzer, topBuilder, node);
          }

          return;
        }
      case HOP:
      case TUMBLE:
      case SESSION:
      case GROUPING_SETS:
      default:
        ImmutableList.Builder<ImmutableBitSet> builder = ImmutableList.builder();
        convertGroupSet(scope, groupAnalyzer, builder, groupExpr);
        topBuilder.add(builder.build());
    }
  }

  private static void convertGroupSet(SqlValidatorScope scope, GroupAnalyzer groupAnalyzer, ImmutableList.Builder<ImmutableBitSet> builder, SqlNode groupExpr) {
    switch (groupExpr.getKind()) {
      case ROLLUP:
      case CUBE:
        List<ImmutableBitSet> operandBitSet = analyzeGroupTuple(scope, groupAnalyzer, ((SqlCall)groupExpr).getOperandList());
        switch (groupExpr.getKind()) {
          case ROLLUP:
            builder.addAll(rollup(operandBitSet));
            return;
          default:
            builder.addAll(cube(operandBitSet));
            return;
        }
      case OTHER:
      case HOP:
      case TUMBLE:
      case SESSION:
      default:
        builder.add(analyzeGroupExpr(scope, groupAnalyzer, groupExpr));
        return;
      case GROUPING_SETS:
        SqlCall call = (SqlCall)groupExpr;
        Iterator var7 = call.getOperandList().iterator();

        while(var7.hasNext()) {
          SqlNode node = (SqlNode)var7.next();
          convertGroupSet(scope, groupAnalyzer, builder, node);
        }

        return;
      case ROW:
        List<ImmutableBitSet> bitSets = analyzeGroupTuple(scope, groupAnalyzer, ((SqlCall)groupExpr).getOperandList());
        builder.add(ImmutableBitSet.union(bitSets));
    }
  }

  private static List<ImmutableBitSet> analyzeGroupTuple(SqlValidatorScope scope, GroupAnalyzer groupAnalyzer, List<SqlNode> operandList) {
    List<ImmutableBitSet> list = new ArrayList();
    Iterator var4 = operandList.iterator();

    while(var4.hasNext()) {
      SqlNode operand = (SqlNode)var4.next();
      list.add(analyzeGroupExpr(scope, groupAnalyzer, operand));
    }

    return list;
  }

  private static ImmutableBitSet analyzeGroupExpr(SqlValidatorScope scope, GroupAnalyzer groupAnalyzer, SqlNode groupExpr) {
    SqlNode expandedGroupExpr = scope.getValidator().expand(groupExpr, scope);
    switch (expandedGroupExpr.getKind()) {
      case OTHER:
        if (expandedGroupExpr instanceof SqlNodeList && ((SqlNodeList)expandedGroupExpr).size() == 0) {
          return ImmutableBitSet.of();
        }
        break;
      case ROW:
        return ImmutableBitSet.union(analyzeGroupTuple(scope, groupAnalyzer, ((SqlCall)expandedGroupExpr).getOperandList()));
    }

    int ref = lookupGroupExpr(groupAnalyzer, expandedGroupExpr);
    if (expandedGroupExpr instanceof SqlIdentifier) {
      SqlIdentifier expr = (SqlIdentifier)expandedGroupExpr;

      assert expr.names.size() >= 2;

      String originalRelName = (String)expr.names.get(0);
      String originalFieldName = (String)expr.names.get(1);
      SqlNameMatcher nameMatcher = scope.getValidator().getCatalogReader().nameMatcher();
      SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
      scope.resolve(ImmutableList.of(originalRelName), nameMatcher, false, resolved);

      assert resolved.count() == 1;

      SqlValidatorScope.Resolve resolve = resolved.only();
      RelDataType rowType = resolve.rowType();
      int childNamespaceIndex = ((SqlValidatorScope.Step)resolve.path.steps().get(0)).i;
      int namespaceOffset = 0;
      if (childNamespaceIndex > 0) {
        SqlValidatorScope ancestorScope = resolve.scope;

        assert ancestorScope instanceof ListScope;

        List<SqlValidatorNamespace> children = ((ListScope)ancestorScope).getChildren();

        for(int j = 0; j < childNamespaceIndex; ++j) {
          namespaceOffset += ((SqlValidatorNamespace)children.get(j)).getRowType().getFieldCount();
        }
      }

      RelDataTypeField field = nameMatcher.field(rowType, originalFieldName);
      int origPos = namespaceOffset + field.getIndex();
      groupAnalyzer.groupExprProjection.put(origPos, ref);
    }

    return ImmutableBitSet.of(new int[]{ref});
  }

  private static int lookupGroupExpr(GroupAnalyzer groupAnalyzer, SqlNode expr) {
    Iterator var2 = Ord.zip(groupAnalyzer.groupExprs).iterator();

    Ord node;
    do {
      if (!var2.hasNext()) {
        switch (expr.getKind()) {
          case HOP:
          case TUMBLE:
          case SESSION:
            groupAnalyzer.extraExprs.add(expr);
          default:
            groupAnalyzer.groupExprs.add(expr);
            return groupAnalyzer.groupExprs.size() - 1;
        }
      }

      node = (Ord)var2.next();
    } while(!((SqlNode)node.e).equalsDeep(expr, Litmus.IGNORE));

    return node.i;
  }

  @VisibleForTesting
  public static ImmutableList<ImmutableBitSet> rollup(List<ImmutableBitSet> bitSets) {
    Set<ImmutableBitSet> builder = new LinkedHashSet();

    while(true) {
      ImmutableBitSet union = ImmutableBitSet.union(bitSets);
      builder.add(union);
      if (union.isEmpty()) {
        return ImmutableList.copyOf(builder);
      }

      bitSets = bitSets.subList(0, bitSets.size() - 1);
    }
  }

  @VisibleForTesting
  public static ImmutableList<ImmutableBitSet> cube(List<ImmutableBitSet> bitSets) {
    Set<List<ImmutableBitSet>> builder = new LinkedHashSet();
    Iterator var2 = bitSets.iterator();

    while(var2.hasNext()) {
      ImmutableBitSet bitSet = (ImmutableBitSet)var2.next();
      builder.add(Arrays.asList(bitSet, ImmutableBitSet.of()));
    }

    Set<ImmutableBitSet> flattenedBitSets = new LinkedHashSet();
    Iterator var6 = Linq4j.product(builder).iterator();

    while(var6.hasNext()) {
      List<ImmutableBitSet> o = (List)var6.next();
      flattenedBitSets.add(ImmutableBitSet.union(o));
    }

    return ImmutableList.copyOf(flattenedBitSets);
  }

  public static CalciteSchema.TypeEntry getTypeEntry(CalciteSchema rootSchema, SqlIdentifier typeName) {
    String name;
    Object path;
    if (typeName.isSimple()) {
      path = ImmutableList.of();
      name = typeName.getSimple();
    } else {
      path = Util.skipLast(typeName.names);
      name = (String)Util.last(typeName.names);
    }

    CalciteSchema schema = rootSchema;
    Iterator var5 = ((List)path).iterator();

    while(true) {
      String p;
      do {
        if (!var5.hasNext()) {
          return schema == null ? null : schema.getType(name, false);
        }

        p = (String)var5.next();
      } while(schema == rootSchema && SqlNameMatchers.withCaseSensitive(true).matches(p, schema.getName()));

      schema = schema.getSubSchema(p, true);
    }
  }

  public static CalciteSchema.TableEntry getTableEntry(SqlValidatorCatalogReader catalogReader, List<String> names) {
    Iterator var2 = catalogReader.getSchemaPaths().iterator();

    while(var2.hasNext()) {
      List<String> schemaPath = (List)var2.next();
      CalciteSchema schema = getSchema(catalogReader.getRootSchema(), Iterables.concat(schemaPath, Util.skipLast(names)), catalogReader.nameMatcher());
      if (schema != null) {
        CalciteSchema.TableEntry entry = getTableEntryFrom(schema, (String)Util.last(names), catalogReader.nameMatcher().isCaseSensitive());
        if (entry != null) {
          return entry;
        }
      }
    }

    return null;
  }

  public static CalciteSchema getSchema(CalciteSchema rootSchema, Iterable<String> schemaPath, SqlNameMatcher nameMatcher) {
    CalciteSchema schema = rootSchema;
    Iterator var4 = schemaPath.iterator();

    do {
      String schemaName;
      do {
        if (!var4.hasNext()) {
          return schema;
        }

        schemaName = (String)var4.next();
      } while(schema == rootSchema && nameMatcher.matches(schemaName, schema.getName()));

      schema = schema.getSubSchema(schemaName, nameMatcher.isCaseSensitive());
    } while(schema != null);

    return null;
  }

  private static CalciteSchema.TableEntry getTableEntryFrom(CalciteSchema schema, String name, boolean caseSensitive) {
    CalciteSchema.TableEntry entry = schema.getTable(name, caseSensitive);
    if (entry == null) {
      entry = schema.getTableBasedOnNullaryFunction(name, caseSensitive);
    }

    return entry;
  }

  public static boolean containsMonotonic(SqlValidatorScope scope) {
    Iterator var1 = children(scope).iterator();

    while(var1.hasNext()) {
      SqlValidatorNamespace ns = (SqlValidatorNamespace)var1.next();
      ns = ns.resolve();
      Iterator var3 = ns.getRowType().getFieldNames().iterator();

      while(var3.hasNext()) {
        String field = (String)var3.next();
        SqlMonotonicity monotonicity = ns.getMonotonicity(field);
        if (monotonicity != null && !monotonicity.mayRepeat()) {
          return true;
        }
      }
    }

    return false;
  }

  private static List<SqlValidatorNamespace> children(SqlValidatorScope scope) {
    return (List)(scope instanceof ListScope ? ((ListScope)scope).getChildren() : ImmutableList.of());
  }

  static boolean containsMonotonic(SelectScope scope, SqlNodeList nodes) {
    Iterator var2 = nodes.iterator();

    SqlNode node;
    do {
      if (!var2.hasNext()) {
        return false;
      }

      node = (SqlNode)var2.next();
    } while(scope.getMonotonicity(node).mayRepeat());

    return true;
  }

  public static SqlOperator lookupSqlFunctionByID(SqlOperatorTable opTab, SqlIdentifier funName, SqlFunctionCategory funcType) {
    if (funName.isSimple()) {
      List<SqlOperator> list = new ArrayList();
      opTab.lookupOperatorOverloads(funName, funcType, SqlSyntax.FUNCTION, list, SqlNameMatchers.withCaseSensitive(funName.isComponentQuoted(0)));
      if (list.size() == 1) {
        return (SqlOperator)list.get(0);
      }
    }

    return null;
  }

  public static Pair<SqlNode, RelDataType> validateExprWithRowType(boolean caseSensitive, SqlOperatorTable operatorTable, RelDataTypeFactory typeFactory, RelDataType rowType, SqlNode expr) {
    String tableName = "_table_";
    SqlSelect select0 = new SqlSelect(SqlParserPos.ZERO, (SqlNodeList)null, new SqlNodeList(Collections.singletonList(expr), SqlParserPos.ZERO), new SqlIdentifier("_table_", SqlParserPos.ZERO), (SqlNode)null, (SqlNodeList)null, (SqlNode)null, (SqlNodeList)null, (SqlNodeList)null, (SqlNode)null, (SqlNode)null, (SqlNodeList)null);
    Prepare.CatalogReader catalogReader = createSingleTableCatalogReader(caseSensitive, "_table_", typeFactory, rowType);
    SqlValidator validator = newValidator(operatorTable, catalogReader, typeFactory, Config.DEFAULT);
    SqlSelect select = (SqlSelect)validator.validate(select0);

    assert select.getSelectList().size() == 1 : "Expression " + expr + " should be atom expression";

    SqlNode node = select.getSelectList().get(0);
    RelDataType nodeType = ((RelDataTypeField)validator.getValidatedNodeType(select).getFieldList().get(0)).getType();
    return Pair.of(node, nodeType);
  }

  public static CalciteCatalogReader createSingleTableCatalogReader(boolean caseSensitive, String tableName, RelDataTypeFactory typeFactory, RelDataType rowType) {
    Properties properties = new Properties();
    properties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), String.valueOf(caseSensitive));
    CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(properties);
    ExplicitRowTypeTable table = new ExplicitRowTypeTable(rowType);
    Map<String, Table> tableMap = Collections.singletonMap(tableName, table);
    CalciteSchema schema = CalciteSchema.createRootSchema(false, false, "", new ExplicitTableSchema(tableMap));
    return new CalciteCatalogReader(schema, new ArrayList(new ArrayList()), typeFactory, connectionConfig);
  }

  private static class ExplicitTableSchema extends AbstractSchema {
    private final Map<String, Table> tableMap;

    ExplicitTableSchema(Map<String, Table> tableMap) {
      this.tableMap = (Map)Objects.requireNonNull(tableMap);
    }

    protected Map<String, Table> getTableMap() {
      return this.tableMap;
    }
  }

  private static class ExplicitRowTypeTable extends AbstractTable {
    private final RelDataType rowType;

    ExplicitRowTypeTable(RelDataType rowType) {
      this.rowType = (RelDataType)Objects.requireNonNull(rowType);
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return this.rowType;
    }
  }

  static class GroupAnalyzer {
    final List<SqlNode> extraExprs = new ArrayList();
    final List<SqlNode> groupExprs = new ArrayList();
    final Map<Integer, Integer> groupExprProjection = new HashMap();

    GroupAnalyzer() {
    }
  }

  public interface Suggester {
    String apply(String var1, int var2, int var3);
  }

  /** @deprecated */
  @Deprecated
  public static class DeepCopier extends SqlScopedShuttle {
    DeepCopier(SqlValidatorScope scope) {
      super(scope);
    }

    public static SqlNodeList copy(SqlValidatorScope scope, SqlNodeList list) {
      return (SqlNodeList)list.accept(new DeepCopier(scope));
    }

    public SqlNode visit(SqlNodeList list) {
      SqlNodeList copy = new SqlNodeList(list.getParserPosition());
      Iterator var3 = list.iterator();

      while(var3.hasNext()) {
        SqlNode node = (SqlNode)var3.next();
        copy.add((SqlNode)node.accept(this));
      }

      return copy;
    }

    protected SqlNode visitScoped(SqlCall call) {
      SqlBasicVisitor.ArgHandler<SqlNode> argHandler = new SqlShuttle.CallCopyingArgHandler( call, true);
      call.getOperator().acceptCall(this, call, false, argHandler);
      return (SqlNode)argHandler.result();
    }

    public SqlNode visit(SqlLiteral literal) {
      return SqlNode.clone(literal);
    }

    public SqlNode visit(SqlIdentifier id) {
      SqlValidator validator = this.getScope().getValidator();
      SqlCall call = validator.makeNullaryCall(id);
      return (SqlNode)(call != null ? call : this.getScope().fullyQualify(id).identifier);
    }

    public SqlNode visit(SqlDataTypeSpec type) {
      return SqlNode.clone(type);
    }

    public SqlNode visit(SqlDynamicParam param) {
      return SqlNode.clone(param);
    }

    public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
      return SqlNode.clone(intervalQualifier);
    }
  }
}
