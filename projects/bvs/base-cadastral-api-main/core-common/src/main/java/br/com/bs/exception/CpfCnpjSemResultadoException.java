package br.com.bs.exception;

import org.springframework.http.HttpStatus;

public class CpfCnpjSemResultadoException extends CpfCnpjAbstractException {

    public static final int code = HttpStatus.NO_CONTENT.value();
    public static final String msg = "Não há resultados para o documento informado";

    public CpfCnpjSemResultadoException() {
        super(code, msg);
    }

}
