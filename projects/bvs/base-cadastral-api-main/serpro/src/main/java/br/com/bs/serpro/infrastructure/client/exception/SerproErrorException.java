package br.com.bs.serpro.infrastructure.client.exception;

import feign.Response;

public class SerproErrorException extends SerproBaseException {

    public SerproErrorException(Response response) {
        super(response);
    }
}
