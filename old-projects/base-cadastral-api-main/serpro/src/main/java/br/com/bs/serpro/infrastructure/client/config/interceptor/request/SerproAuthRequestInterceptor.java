package br.com.bs.serpro.infrastructure.client.config.interceptor.request;

import br.com.bs.serpro.infrastructure.auth.SerproAuthService;
import feign.RequestInterceptor;
import feign.RequestTemplate;

public class SerproAuthRequestInterceptor implements RequestInterceptor {

    private final SerproAuthService serproAuthService;

    public SerproAuthRequestInterceptor(SerproAuthService serproAuthService) {
        this.serproAuthService = serproAuthService;
    }

    @Override
    public void apply(RequestTemplate requestTemplate) {
        requestTemplate.header("Authorization", "Bearer " + this.serproAuthService.getToken());
    }

}
