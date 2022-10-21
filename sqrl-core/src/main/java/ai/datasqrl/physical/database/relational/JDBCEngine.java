package ai.datasqrl.physical.database.relational;

import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.physical.EngineCapability;
import ai.datasqrl.physical.ExecutionEngine;

import java.util.EnumMap;
import java.util.EnumSet;

import static ai.datasqrl.physical.EngineCapability.*;

public class JDBCEngine extends ExecutionEngine.Impl {

    public static final EnumMap<JDBCConfiguration.Dialect,EnumSet<EngineCapability>> CAPABILITIES_BY_DIALECT = new EnumMap<JDBCConfiguration.Dialect, EnumSet<EngineCapability>>(JDBCConfiguration.Dialect.class);
    static {
        CAPABILITIES_BY_DIALECT.put(JDBCConfiguration.Dialect.POSTGRES,
                EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK));
    }

    public JDBCEngine(JDBCConfiguration.Dialect dialect) {
        super(Type.DATABASE, CAPABILITIES_BY_DIALECT.get(dialect));
    }
}
