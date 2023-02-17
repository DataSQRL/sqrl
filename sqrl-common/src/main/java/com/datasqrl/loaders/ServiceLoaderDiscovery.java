package com.datasqrl.loaders;

import lombok.NonNull;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Predicate;

public class ServiceLoaderDiscovery {

    public static <L> Optional<L> findFirst(@NonNull Class<L> clazz, @NonNull Predicate<L> condition) {
        ServiceLoader<L> factories = ServiceLoader.load(clazz);
        for (L factory : factories) {
            if (condition.test(factory)) {
                return Optional.of(factory);
            }
        }
        return Optional.empty();
    }

    public static <L> Optional<L> findFirst(@NonNull Class<L> clazz, @NonNull Function<L,String> key, @NonNull String value) {
        return findFirst(clazz, l -> key.apply(l).equalsIgnoreCase(value));
    }

    public static <L> Optional<L> findFirst(@NonNull Class<L> clazz, @NonNull Function<L,String> key1, @NonNull String value1,
                                     @NonNull Function<L,String> key2, @NonNull String value2) {
        return findFirst(clazz, l -> key1.apply(l).equalsIgnoreCase(value1) && key2.apply(l).equalsIgnoreCase(value2));
    }


}
