package com.datasqrl.engine.database;

/**
 * Used by {@link DatabaseEngine} to keep track of information on created tables
 */
public interface EngineCreateTable {

  EngineCreateTable NONE = new EngineCreateTable() {};
  String TABLE_NAME_KEY = "table-name";

}
