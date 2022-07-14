package ai.datasqrl.plan.local;

import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.JoinAssignment;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.QueryAssignment;
import ai.datasqrl.parse.tree.SelectItem;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.name.NamePath;

public class Errors {

  public static final Error<DistinctAssignment> DISTINCT_TABLE_NESTED = new Error<>();
  public static final Error<? super NamePath> DISTINCT_NOT_ON_ROOT = new Error<>();
  public static final Error<? super TableNode> TABLE_NOT_FOUND = new Error<>();
  public static final Error<? super ExpressionAssignment> EXPRESSION_ON_ROOT = new Error<>();
  public static Error<? super ExpressionAssignment> UNASSIGNABLE_EXPRESSION  ;
  public static Error<? super JoinAssignment> UNASSIGNABLE_TABLE;
  public static Error<? super QueryAssignment> UNASSIGNABLE_QUERY_TABLE;
  public static Error<? super QueryAssignment> QUERY_EXPRESSION_ON_ROOT;
  public static Error<? super JoinAssignment> JOIN_ON_ROOT;
  public static Error<? super Node> NOT_YET_IMPLEMENTED;
  public static Error<? super SelectItem> DUPLICATE_COLUMN_NAME;
  public static Error<? super SelectItem> INVALID_STAR_ALIAS;
  public static Error<? super TableNode> TABLE_PATH_TYPE_INVALID;
  public static Error<? super TableNode> TABLE_PATH_NOT_FOUND;
  public static Error<? super TableNode> TABLE_PATH_AMBIGUOUS;
  public static Error<? super Identifier> PATH_NOT_FOUND;
  public static Error<? super Identifier> PATH_AMBIGUOUS;
  public static Error<? super Identifier> IDENTIFIER_MUST_BE_COLUMN;
  public static Error<? super Identifier> PATH_NOT_TO_ONE;
  public static Error<? super Identifier> PATH_NOT_ALLOWED;
  public static Error<? super Expression> GROUP_BY_COLUMN_MISSING;
  public static Error<? super SelectItem> UNNAMED_QUERY_COLUMN;

  public static class Error<T> {

  }
}
