package com.datasqrl.function;

/**
 * A function that preserves one of its inputs, meaning it does not substantially change the value.
 *
 * This is used in primary key and timestamp determination to identify when the value has been preserved.
 */
public interface InputPreservingFunction extends SqrlFunction {

    int preservedOperandIndex();

}
