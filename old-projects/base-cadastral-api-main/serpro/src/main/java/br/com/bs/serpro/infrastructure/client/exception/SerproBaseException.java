package br.com.bs.serpro.infrastructure.client.exception;

import feign.Response;

public abstract class SerproBaseException extends RuntimeException{

    protected final Response response;

    protected SerproBaseException(Response response) {
        this.response = response;
    }

    public Response getResponse() {
        return response;
    }
}
