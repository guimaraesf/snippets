package br.com.bs.exception;

public abstract class CpfCnpjAbstractException extends ApiException {

    public CpfCnpjAbstractException(String s) {
        super(s);
    }

    public CpfCnpjAbstractException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public CpfCnpjAbstractException(int status, String s) {
        super(status, s);
    }

    public CpfCnpjAbstractException(int status, String s, Throwable throwable) {
        super(status, s, throwable);
    }
}
