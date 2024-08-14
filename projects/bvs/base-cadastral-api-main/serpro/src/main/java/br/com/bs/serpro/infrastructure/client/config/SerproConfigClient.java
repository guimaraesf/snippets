package br.com.bs.serpro.infrastructure.client.config;

import br.com.bs.serpro.infrastructure.auth.SerproAuthService;
import br.com.bs.serpro.infrastructure.client.config.error.SerproCustomErrorDecoder;
import br.com.bs.serpro.infrastructure.client.config.interceptor.request.SerproAuthRequestInterceptor;
import feign.Logger;
import feign.RequestInterceptor;
import feign.codec.ErrorDecoder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SerproConfigClient {

    @Bean
    public Logger.Level feignLoggerLevel() {
        return Logger.Level.FULL;
    }

    @Bean
    public RequestInterceptor requestInterceptors(SerproAuthService serproAuthService) {
        return new SerproAuthRequestInterceptor(serproAuthService);
    }

    @Bean
    public ErrorDecoder errorDecoder(){
        return new SerproCustomErrorDecoder();
    }

}
