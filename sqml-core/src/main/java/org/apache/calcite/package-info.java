/**
 * Not practically overridable:
 *  - RelDataTypeField: Constructor called directly during rel building
 *  - Project (even via ProjectFactory): There are hard coded default factories that cannot be
 *    overridden
 */
package org.apache.calcite;
