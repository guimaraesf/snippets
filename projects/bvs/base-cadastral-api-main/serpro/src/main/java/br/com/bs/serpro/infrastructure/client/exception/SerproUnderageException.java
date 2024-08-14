package br.com.bs.serpro.infrastructure.client.exception;

import feign.Response;

public class SerproUnderageException extends SerproBaseException{

    public SerproUnderageException(Response response) {
        super(response);
    }
}
