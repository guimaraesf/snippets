package br.com.bs.serpro.infrastructure.client.exception;

import feign.Response;

public class SerproDocumentNotFoundException extends SerproBaseException{

    public SerproDocumentNotFoundException(Response response) {
        super(response);
    }
}
