/**
 * This contains the old Flink RelNode to SqlNode mapping code.
 * A lot of this is now deprecated because we are constructing the Flink
 * plan directly.
 * However, some of the SqlNode utility methods are still being used.
 * Hence, this requires a careful refactor.
 *
 * TODO: Refactor
 */
package com.datasqrl.engine.stream.flink.sql;