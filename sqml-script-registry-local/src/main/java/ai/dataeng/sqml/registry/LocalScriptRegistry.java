package ai.dataeng.sqml.registry;

import ai.dataeng.sqml.tree.Script;
import java.util.HashMap;
import java.util.Map;

public class LocalScriptRegistry extends ScriptRegistry {
  private final Map<String, Script> scripts;

  public LocalScriptRegistry(Map<String, Script> scripts) {
    this.scripts = scripts;
  }

  public static Builder newScriptRegistry() {
    return new Builder();
  }

  @Override
  public Script getScript(String name) {
    return scripts.get(name);
  }

  public static class Builder extends ScriptRegistry.Builder {
    private Map<String, Script> scripts = new HashMap<>();
    public Builder script(String name, Script tokens) {
      scripts.put(name, tokens);
      return this;
    }

    @Override
    public ScriptRegistry build() {
      return new LocalScriptRegistry(scripts);
    }
  }
}
