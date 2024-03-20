package com.datasqrl.plan.rules;

import com.datasqrl.error.NotYetImplementedException;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.JoinModifier;

@Value
@AllArgsConstructor
public class JoinAnalysis {

  @NonNull Type type;
  @NonNull Side side;
  boolean isFlipped;

  public JoinAnalysis(Type type, Side side) {
    this(type, side, false);
  }

  public JoinAnalysis flip() {
    return new JoinAnalysis(type, side.flip(), true);
  }

  public boolean isA(Type type) {
    return this.type==type;
  }

  public JoinAnalysis makeA(Type type) {
    Preconditions.checkArgument(this.type.isCompatible(type), "Join types not compatible: %s vs %s", this.type, type);
    if (this.type==type) return this;
    return new JoinAnalysis(type,side,isFlipped);
  }

  public JoinAnalysis makeGeneric() {
    Preconditions.checkArgument(this.type==Type.DEFAULT || isGeneric(), "Join types not compatible: %s vs %s", this.type, type);
    if (isGeneric()) return this;
    if (side==Side.NONE) return new JoinAnalysis(Type.INNER, Side.NONE, isFlipped);
    else return new JoinAnalysis(Type.OUTER, side, isFlipped);
  }

  public boolean canBe(Type type) {
    return this.type==Type.DEFAULT || isA(type);
  }

  public boolean canBe(Type type, Side side) {
    return canBe(type) && isA(side);
  }

  public boolean isA(Side side) {
    return this.side==side;
  }

  public Side getOriginalSide() {
    Side orgSide = this.side;
    if (isFlipped) orgSide = orgSide.flip();
    return orgSide;
  }

  public boolean isGeneric() {
    return this.type==Type.INNER || this.type==Type.OUTER;
  }

  public JoinRelType export() {
    Preconditions.checkArgument(type!=Type.DEFAULT);
    if (side==Side.LEFT) return JoinRelType.LEFT;
    if (side==Side.RIGHT) return JoinRelType.RIGHT;
    if (type==Type.OUTER) return JoinRelType.FULL;
    return JoinRelType.INNER;
  }


  public enum Type {
    DEFAULT, INNER, OUTER, TEMPORAL, INTERVAL;

    public boolean isCompatible(@NonNull Type other) {
      if (this==DEFAULT) return true;
      if ((this==INNER || this==OUTER) && (other==INTERVAL)) return true;
      return this==other;
    }

  }

  public enum Side {
    LEFT, RIGHT, NONE;

    public Side flip() {
      switch (this) {
        case LEFT: return RIGHT;
        case RIGHT: return LEFT;
        case NONE: return NONE;
        default: throw new IllegalStateException("Not a side: " + this);
      }
    }

  }

  public static JoinAnalysis of(JoinRelType join, JoinModifier joinModifier) {
    Side side = determineSide(join);
    Type joinType = determineJoinType(join, joinModifier);
    return new JoinAnalysis(joinType, side);
  }

  private static Side determineSide(JoinRelType join) {
    if (join == JoinRelType.LEFT) {
      return Side.LEFT;
    } else if (join == JoinRelType.RIGHT) {
      return Side.RIGHT;
    } else {
      return Side.NONE;
    }
  }

  private static Type determineJoinType(JoinRelType join, JoinModifier joinModifier) {
    switch (joinModifier) {
      case TEMPORAL:
        return Type.TEMPORAL;
      case INTERVAL:
        return Type.INTERVAL;
      case DEFAULT:
        if (join==JoinRelType.LEFT || join==JoinRelType.RIGHT) return Type.INNER;
        else return Type.DEFAULT;
      case OUTER:
      case NONE:
        if (join == JoinRelType.INNER) {
          return Type.INNER;
        } else {
          return Type.OUTER;
        }
      default:
        throw new NotYetImplementedException("Unsupported join modifier: " + joinModifier);
    }
  }
}
