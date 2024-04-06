package br.com.bs.exception;


import org.springframework.http.HttpStatus;

public class ApiException extends RuntimeException {
    private int statusCode;

    public ApiException(String s) {
        this(HttpStatus.INTERNAL_SERVER_ERROR.value(), s);
    }

    public ApiException(String s, Throwable throwable) {
        this(HttpStatus.INTERNAL_SERVER_ERROR.value(), s, throwable);
    }

    public ApiException(int status, String s) {
        super(s);
        this.statusCode = status;
    }

    public ApiException(int status, String s, Throwable throwable) {
        super(s, throwable);
        this.statusCode = status;
    }

    public int getStatusCode() {
        return statusCode;
    }
}