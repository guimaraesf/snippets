package br.com.bs.serpro.infrastructure.client.config;

import br.com.bs.serpro.infrastructure.client.config.error.SerproCustomErrorDecoder;
import feign.Logger;
import feign.RequestInterceptor;
import feign.auth.BasicAuthRequestInterceptor;
import feign.codec.Encoder;
import feign.codec.ErrorDecoder;
import feign.form.spring.SpringFormEncoder;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.http.HttpMessageConverters;
import org.springframework.cloud.openfeign.support.SpringEncoder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SerproAuthConfigClient {

    private final String clientId;
    private final String clientSecret;

    public SerproAuthConfigClient(
            @Value("${serpro.auth.client-id}") String clientId,
            @Value("${serpro.auth.client-secret}") String clientSecret
    ) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    @Bean
    public Encoder encoder(ObjectFactory<HttpMessageConverters> converters) {
        return new SpringFormEncoder(new SpringEncoder(converters));
    }

    @Bean
    public Logger.Level feignLoggerLevel() {
        return Logger.Level.FULL;
    }

    @Bean
    public RequestInterceptor requestInterceptors() {
        return new BasicAuthRequestInterceptor(this.clientId, this.clientSecret);
    }

    @Bean
    public ErrorDecoder errorDecoder(){
        return new SerproCustomErrorDecoder();
    }

}
