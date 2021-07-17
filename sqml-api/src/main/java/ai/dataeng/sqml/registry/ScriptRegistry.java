package ai.dataeng.sqml.registry;

import ai.dataeng.sqml.tree.Script;

public abstract class ScriptRegistry {

  public abstract Script getScript(String name);

  public static class Builder {

    public ScriptRegistry build() {
      return null;
    }
  }
}
