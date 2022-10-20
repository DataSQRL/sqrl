package org.apache.calcite.sql.validate;

import static org.apache.calcite.util.Static.RESOURCE;

import ai.datasqrl.SqrlCalciteCatalogReader;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.HasToTable;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.SQRLTable;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

/**
 * Visitor which derives the type of a given {@link SqlNode}.
 *
 * <p>Each method must return the derived type. This visitor is basically a
 * single-use dispatcher; the visit is never recursive.
 */
@AllArgsConstructor
public class DeriveTypeVisitor2 implements SqlVisitor<RelDataType> {
  private final SqrlValidatorImpl validator;
  private final RelDataTypeFactory typeFactory;
  private final SqlValidatorCatalogReader catalogReader;
  private final SqlValidatorScope scope;

  public RelDataType visit(SqlLiteral literal) {
    return literal.createSqlType(typeFactory);
  }

  public RelDataType visit(SqlCall call) {
    final SqlOperator operator = call.getOperator();
    return operator.deriveType(validator, scope, call);
  }

  public RelDataType visit(SqlNodeList nodeList) {
    // Operand is of a type that we can't derive a type for. If the
    // operand is of a peculiar type, such as a SqlNodeList, then you
    // should override the operator's validateCall() method so that it
    // doesn't try to validate that operand as an expression.
    throw Util.needToImplement(nodeList);
  }

  public RelDataType visit(SqlIdentifier id) {
    // First check for builtin functions which don't have parentheses,
    // like "LOCALTIME".
    final SqlCall call = validator.makeNullaryCall(id);
    if (call != null) {
      return call.getOperator().validateOperands(
          validator,
          scope,
          call);
    }

    RelDataType type = null;
    if (!(scope instanceof EmptyScope)) {
      id = scope.fullyQualify(id).identifier;
      //SQRL: Don't do usual checking, just grab it from the qualified anmespace
      if (id.names.size() > 2) {

        HasToTable table = scope.fullyQualify(id).namespace.getTable().unwrap(HasToTable.class);

        List<Field> fields = table.getToTable()
            .walkField(scope.fullyQualify(id).suffix());
        Column column = (Column) fields.get(fields.size()-1);
        type = column.getType();
        type =
            SqlTypeUtil.addCharsetAndCollation(
                type,
                typeFactory);
        return type;
      }
    }

    // Resolve the longest prefix of id that we can
    int i;
    for (i = id.names.size() - 1; i > 0; i--) {
      // REVIEW jvs 9-June-2005: The name resolution rules used
      // here are supposed to match SQL:2003 Part 2 Section 6.6
      // (identifier chain), but we don't currently have enough
      // information to get everything right.  In particular,
      // routine parameters are currently looked up via resolve;
      // we could do a better job if they were looked up via
      // resolveColumn.

      final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
      final SqlValidatorScope.ResolvedImpl resolved =
          new SqlValidatorScope.ResolvedImpl();
      scope.resolve(id.names.subList(0, i), nameMatcher, false, resolved);
      if (resolved.count() == 1) {
        // There's a namespace with the name we seek.
        final SqlValidatorScope.Resolve resolve = resolved.only();
        type = resolve.rowType();
        for (SqlValidatorScope.Step p : Util.skip(resolve.path.steps())) {
          type = type.getFieldList().get(p.i).getType();
        }
        break;
      }
    }

    // Give precedence to namespace found, unless there
    // are no more identifier components.
    if (type == null || id.names.size() == 1) {
      // See if there's a column with the name we seek in
      // precisely one of the namespaces in this scope.
      RelDataType colType = scope.resolveColumn(id.names.get(0), id);
      if (colType != null) {
        type = colType;
      }
      ++i;
    }

    if (type == null) {
      final SqlIdentifier last = id.getComponent(i - 1, i);
      throw validator.newValidationError(last,
          RESOURCE.unknownIdentifier(last.toString()));
    }


    RelDataType colType = scope.resolveColumn(id.names.get(0), id);
    if (colType != null) {
      type =
          SqlTypeUtil.addCharsetAndCollation(
              type,
              typeFactory);
      return type;
    }
//    if (!(scope instanceof EmptyScope) && scope.fullyQualify(id).identifier.names.size() > 2) {
////      this.scope.fullyQualify(id).namespace.getTable()
//      SqlQualified q = scope.fullyQualify(id);
//      SqlValidatorNamespace ns = q.namespace;
//      SQRLTable table = q.namespace.getTable()
//          .unwrap(HasToTable.class).getToTable();
//      SQRLTable target = table.walkTable(NamePath.of(q.identifier.names.subList(1, q.identifier.names.size() - 1)
//          .toArray(new String[]{}))).get();
//      RelDataTypeField f = target.getVt().getRowType().getField(q.identifier.names.get(q.identifier.names.size() - 1), false, false);
////      this.scope.fullyQualify(id).namespace.getTable()
////              .unwrap(HasToTable.class)
////              .getToTable().getVt().getRowType()
////              .getField(id.names.get(id.names.size() - 1), false, false);
//      if (f != null) {
//        type = f.getType();
//        type =
//            SqlTypeUtil.addCharsetAndCollation(
//                type,
//                typeFactory);
//        return type;
//      } else {
//        throw new RuntimeException("Could not find field");
//      }
//    }

    // Resolve rest of identifier
    for (; i < id.names.size(); i++) {
      String name = id.names.get(i);
      final RelDataTypeField field;
      if (name.equals("")) {
        // The wildcard "*" is represented as an empty name. It never
        // resolves to a field.
        name = "*";
        field = null;
      } else {
        final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
        field = nameMatcher.field(type, name);
      }
      if (field == null) {
        throw validator.newValidationError(id.getComponent(i),
            RESOURCE.unknownField(name));
      }
      type = field.getType();
    }
    type =
        SqlTypeUtil.addCharsetAndCollation(
            type,
            typeFactory);
    return type;
  }

  public RelDataType visit(SqlDataTypeSpec dataType) {
    // Q. How can a data type have a type?
    // A. When it appears in an expression. (Say as the 2nd arg to the
    //    CAST operator.)
    validator.validateDataType(dataType);
    return dataType.deriveType(validator);
  }

  public RelDataType visit(SqlDynamicParam param) {
    return validator.unknownType;
  }

  public RelDataType visit(SqlIntervalQualifier intervalQualifier) {
    return typeFactory.createSqlIntervalType(intervalQualifier);
  }
}
