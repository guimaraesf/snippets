package br.com.bs.serpro.infrastructure.client.exception;

import feign.Response;

public class SerproForbiddenException extends SerproBaseException{

    public SerproForbiddenException(Response response) {
        super(response);
    }
}
