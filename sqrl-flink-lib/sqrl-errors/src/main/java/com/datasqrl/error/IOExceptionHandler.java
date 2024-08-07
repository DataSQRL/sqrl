package com.datasqrl.error;

import java.io.IOException;

public class IOExceptionHandler implements ErrorHandler<IOException> {

    @Override
    public ErrorMessage handle(IOException e, ErrorLocation baseLocation) {
        return new ErrorMessage.Implementation(ErrorCode.IOEXCEPTION, e.getMessage(), baseLocation, ErrorMessage.Severity.FATAL);
    }

    @Override
    public Class getHandleClass() {
        return IOException.class;
    }
}
