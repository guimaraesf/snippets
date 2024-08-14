package br.com.bs.serpro.infrastructure.client.exception;

import feign.Response;

public class SerproInvalidDocumentException extends SerproBaseException {

    public SerproInvalidDocumentException(Response response) {
        super(response);
    }
}
