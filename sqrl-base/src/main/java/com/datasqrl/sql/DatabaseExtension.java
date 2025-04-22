package com.datasqrl.sql;

import java.util.Set;

import com.datasqrl.canonicalizer.Name;

/**
 * Some databases require extensions to be loaded to support certain types
 * or operators. This interface helps discover those.
 *
 * Service loader interface
 */
public interface DatabaseExtension {
    Class typeClass();

    Set<Name> operators();

    String getExtensionDdl();
}
