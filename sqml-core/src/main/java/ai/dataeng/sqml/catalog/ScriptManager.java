package ai.dataeng.sqml.catalog;

import ai.dataeng.sqml.planner.Script;
import java.util.Optional;

public interface ScriptManager {

  Optional<Script> getScript(String scriptName);

  void addMainScript(Script result);
}
