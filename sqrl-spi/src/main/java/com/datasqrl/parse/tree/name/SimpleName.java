package com.datasqrl.parse.tree.name;

public class SimpleName extends AbstractName {

  private final String name;

  SimpleName(String name) {
    this.name = validateName(name);
  }

  @Override
  public String getCanonical() {
    return name;
  }

  @Override
  public String getDisplay() {
    return name;
  }
}
