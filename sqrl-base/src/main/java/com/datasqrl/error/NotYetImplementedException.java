package com.datasqrl.error;

import com.google.auto.service.AutoService;

public class NotYetImplementedException extends RuntimeException {

    public NotYetImplementedException(String message) {
        super(message);
    }

    public static void trigger(String msg) {
        throw new NotYetImplementedException(msg);
    }

    @AutoService(ErrorHandler.class)
    public static class Handler implements ErrorHandler<NotYetImplementedException> {

        @Override
        public ErrorMessage handle(NotYetImplementedException e, ErrorLocation baseLocation) {
            return new ErrorMessage.Implementation(ErrorCode.NOT_YET_IMPLEMENTED, e.getMessage(), baseLocation, ErrorMessage.Severity.FATAL);
        }

        @Override
        public Class getHandleClass() {
            return NotYetImplementedException.class;
        }
    }


}
