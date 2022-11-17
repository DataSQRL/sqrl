package ai.datasqrl.config.error;

public interface ErrorHandler<E extends Exception> {

  ErrorMessage handle(E e, ErrorEmitter emitter);
}
