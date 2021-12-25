package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.parser.QueryParser;

public interface QueryParserProvider {
  QueryParser getQueryParser();
}
