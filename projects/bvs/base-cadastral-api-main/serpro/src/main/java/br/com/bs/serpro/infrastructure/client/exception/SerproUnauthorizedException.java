package br.com.bs.serpro.infrastructure.client.exception;

import feign.Response;

public class SerproUnauthorizedException extends SerproBaseException{

    public SerproUnauthorizedException(Response response) {
        super(response);
    }
}
