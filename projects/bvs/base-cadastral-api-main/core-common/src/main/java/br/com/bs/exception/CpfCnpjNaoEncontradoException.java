package br.com.bs.exception;

import org.springframework.http.HttpStatus;

public class CpfCnpjNaoEncontradoException extends CpfCnpjAbstractException {

    public static final int code = HttpStatus.NO_CONTENT.value();
    public static final String msg = "Documento informado inexistente na base de dados para contexto <nome>";

    public CpfCnpjNaoEncontradoException() {
        super(code, msg);
    }

}
