package br.com.bs.exception;

import org.springframework.http.HttpStatus;

public class ApiTimeoutException extends ApiException {

    public static final int code = HttpStatus.REQUEST_TIMEOUT.value();
    public static final String msg = HttpStatus.REQUEST_TIMEOUT.getReasonPhrase();

    public ApiTimeoutException(String s) {
        super(code, s);
    }

}
