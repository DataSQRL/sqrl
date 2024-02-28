package com.datasqrl.sql;

import java.util.Set;

// Service loader interface
public interface PgExtension {
    Class typeClass();

    Set<String> operators();

    SqlDDLStatement getExtensionDdl();
}
