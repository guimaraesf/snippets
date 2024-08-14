package br.com.bs.exception;

import org.springframework.http.HttpStatus;

public class CpfCnpjMenorIdadeException extends CpfCnpjAbstractException {

    public static final int code = HttpStatus.BAD_REQUEST.value();
    public static final String msg = "Documento informado é MENOR de idade, não é passível dessa consulta !";

    public CpfCnpjMenorIdadeException() {
        super(code, msg);
    }

    public CpfCnpjMenorIdadeException(Throwable cause) {
        super(code, msg, cause);
    }
}
