package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.catalog.ScriptManager;
import ai.dataeng.sqml.importer.DatasetManager;
import ai.dataeng.sqml.parser.ScriptParser;

public interface ScriptManagerProvider {
  ScriptManager createScriptManager();
}
