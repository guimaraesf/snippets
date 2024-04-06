package br.com.bs.serpro.infrastructure.client.config.error;

import br.com.bs.serpro.infrastructure.client.exception.*;
import feign.Response;
import feign.codec.ErrorDecoder;

public class SerproCustomErrorDecoder implements ErrorDecoder {

    @Override
    public Exception decode(String methodKey, Response response) {
        return convertToException(response);
    }

    private static SerproBaseException convertToException(Response response) {
        switch (response.status()) {
            case 400:
                return new SerproInvalidDocumentException(response);
            case 401:
                return new SerproUnauthorizedException(response);
            case 403:
                return new SerproForbiddenException(response);
            case 404:
                return new SerproDocumentNotFoundException(response);
            case 422:
                return new SerproUnderageException(response);
            default:
                return new SerproErrorException(response);
        }
    }

}
