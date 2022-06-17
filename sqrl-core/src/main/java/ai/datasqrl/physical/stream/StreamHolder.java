package ai.datasqrl.physical.stream;

public interface StreamHolder<T> {

    <R> StreamHolder<R> mapWithError(FunctionWithError<T,R> function, String errorName, Class<R> clazz);

    void printSink();

}
