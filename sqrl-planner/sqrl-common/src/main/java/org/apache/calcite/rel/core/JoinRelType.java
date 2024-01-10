/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.core;

import java.util.Locale;
/**
 * Copied from Calcite: - Added temporal join
 */

/**
 * Enumeration of join types.
 */
public enum JoinRelType {
  /**
   * Inner join.
   */
  INNER,

  /**
   * Left-outer join.
   */
  LEFT,

  /**
   * Right-outer join.
   */
  RIGHT,

  /**
   * Full-outer join.
   */
  FULL,

  /**
   * Semi-join.
   *
   * <p>For example, {@code EMP semi-join DEPT} finds all {@code EMP} records
   * that have a corresponding {@code DEPT} record:
   *
   * <blockquote><pre>
   * SELECT * FROM EMP
   * WHERE EXISTS (SELECT 1 FROM DEPT
   *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
   * </blockquote>
   */
  SEMI,

  /**
   * Anti-join (also known as Anti-semi-join).
   *
   * <p>For example, {@code EMP anti-join DEPT} finds all {@code EMP} records
   * that do not have a corresponding {@code DEPT} record:
   *
   * <blockquote><pre>
   * SELECT * FROM EMP
   * WHERE NOT EXISTS (SELECT 1 FROM DEPT
   *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
   * </blockquote>
   */
  ANTI,

  TEMPORAL,
  INTERVAL,
  DEFAULT,
  LEFT_DEFAULT,
  LEFT_TEMPORAL,
  LEFT_INTERVAL,
  RIGHT_DEFAULT,
  RIGHT_TEMPORAL,
  RIGHT_INTERVAL,
  ;

  /** Lower-case name. */
  public final String lowerName = name().toLowerCase(Locale.ROOT);

  /**
   * Returns whether a join of this type may generate NULL values on the
   * right-hand side.
   */
  public boolean generatesNullsOnRight() {
    return isLeft() || (this == FULL);
  }

  /**
   * Returns whether a join of this type may generate NULL values on the
   * left-hand side.
   */
  public boolean generatesNullsOnLeft() {
    return isRight() || (this == FULL);
  }

  /**
   * Returns whether a join of this type is an outer join, returns true if the join type may
   * generate NULL values, either on the left-hand side or right-hand side.
   */
  public boolean isOuterJoin() {
    return generatesNullsOnRight() || generatesNullsOnLeft();
  }

  /**
   * Swaps left to right, and vice versa.
   */
  public JoinRelType swap() {
    switch (this) {
      case LEFT:
        return RIGHT;
      case LEFT_DEFAULT:
        return RIGHT_DEFAULT;
      case LEFT_INTERVAL:
        return RIGHT_INTERVAL;
      case LEFT_TEMPORAL:
        return RIGHT_TEMPORAL;
      case RIGHT:
        return LEFT;
      case RIGHT_DEFAULT:
        return LEFT_DEFAULT;
      case RIGHT_INTERVAL:
        return LEFT_INTERVAL;
      case RIGHT_TEMPORAL:
        return LEFT_TEMPORAL;
      default:
        return this;
    }
  }

  /** Returns whether this join type generates nulls on side #{@code i}. */
  public boolean generatesNullsOn(int i) {
    switch (i) {
      case 0:
        return generatesNullsOnLeft();
      case 1:
        return generatesNullsOnRight();
      default:
        throw new IllegalArgumentException("invalid: " + i);
    }
  }

  /** Returns a join type similar to this but that does not generate nulls on
   * the left. */
  public JoinRelType cancelNullsOnLeft() {
    switch (this) {
      case RIGHT:
        return INNER;
      case RIGHT_DEFAULT:
        return DEFAULT;
      case RIGHT_INTERVAL:
        return INTERVAL;
      case RIGHT_TEMPORAL:
        return TEMPORAL;
      case FULL:
        return LEFT;
      default:
        return this;
    }
  }

  /** Returns a join type similar to this but that does not generate nulls on
   * the right. */
  public JoinRelType cancelNullsOnRight() {
    switch (this) {
      case LEFT:
        return INNER;
      case LEFT_DEFAULT:
        return DEFAULT;
      case LEFT_INTERVAL:
        return INTERVAL;
      case LEFT_TEMPORAL:
        return TEMPORAL;
      case FULL:
        return RIGHT;
      default:
        return this;
    }
  }

  public boolean projectsRight() {
    return this != SEMI && this != ANTI;
  }

  /** Returns whether this join type accepts pushing predicates from above into its predicate. */
  public boolean canPushIntoFromAbove() {
    return isInner() || (this == SEMI);
  }

  /** Returns whether this join type accepts pushing predicates from above into its left input. */
  public boolean canPushLeftFromAbove() {
    return isInner() || isLeft() || (this == SEMI) || (this == ANTI);
  }

  /** Returns whether this join type accepts pushing predicates from above into its right input. */
  public boolean canPushRightFromAbove() {
    return isInner() || isRight();
  }

  /** Returns whether this join type accepts pushing predicates from within into its left input. */
  public boolean canPushLeftFromWithin() {
    return isInner() || isRight() || (this == SEMI);
  }

  /** Returns whether this join type accepts pushing predicates from within into its right input. */
  public boolean canPushRightFromWithin() {
    return isInner() || isLeft() || (this == SEMI);
  }

  public boolean isLeft() {
    return (this == LEFT) || (this == LEFT_DEFAULT) || (this == LEFT_INTERVAL) ||  (this == LEFT_TEMPORAL);
  }

  public boolean isInner() {
    return (this == INNER) || (this == DEFAULT) || (this == TEMPORAL) || (this == INTERVAL);
  }

  public boolean isRight() {
    return (this == RIGHT) || (this == RIGHT_DEFAULT) || (this == RIGHT_INTERVAL) || (this == RIGHT_TEMPORAL);
  }
}
