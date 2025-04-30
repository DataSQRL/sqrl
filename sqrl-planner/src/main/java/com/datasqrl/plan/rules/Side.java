package com.datasqrl.plan.rules;

public enum Side {
    LEFT, RIGHT, NONE;

    public Side flip() {
      return switch (this) {
    case LEFT -> RIGHT;
    case RIGHT -> LEFT;
    case NONE -> NONE;
    default -> throw new IllegalStateException("Not a side: " + this);
    };
    }

  }