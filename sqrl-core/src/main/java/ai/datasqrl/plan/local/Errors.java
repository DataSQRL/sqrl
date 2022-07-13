package ai.datasqrl.plan.local;

import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.JoinAssignment;
import ai.datasqrl.parse.tree.QueryAssignment;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.name.NamePath;

public class Errors {

  public static final Error<DistinctAssignment> DISTINCT_TABLE_NESTED = new Error<>();
  public static final Error<? super NamePath> DISTINCT_NOT_ON_ROOT = new Error<>();
  public static final Error<? super TableNode> TABLE_NOT_FOUND = new Error<>();
  public static Error<? super ExpressionAssignment> UNASSIGNABLE_EXPRESSION  ;
  public static Error<? super JoinAssignment> UNASSIGNABLE_TABLE;
  public static Error<? super QueryAssignment> UNASSIGNABLE_QUERY_TABLE;

  public static class Error<T> {

  }
}
