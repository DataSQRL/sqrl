package com.datasqrl.util;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;

@Value
public class ReturnFlag<T> {

    @NonNull
    private final Status status;
    private final T value;


    public static<T> ReturnFlag<T> empty() {
        return new ReturnFlag<>(Status.EMPTY, null);
    }

    public static<T> ReturnFlag<T> invalid() {
        return new ReturnFlag<>(Status.INVALID, null);
    }

    public static<T> ReturnFlag<T> of(T t) {
        return new ReturnFlag<>(Status.VALUE, t);
    }

    public boolean isEmpty() {
        return status == Status.EMPTY;
    }

    public boolean isPresent() {
        return status == Status.VALUE;
    }

    public boolean isInvalid() {
        return status == Status.INVALID;
    }

    public T get() {
        Preconditions.checkState(status==Status.VALUE);
        return value;
    }


    public enum Status { EMPTY, INVALID, VALUE }

}
