package br.com.bs.serpro.infrastructure.client;

import br.com.bs.serpro.infrastructure.client.config.SerproAuthConfigClient;
import br.com.bs.serpro.infrastructure.auth.data.TokenData;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;

import java.util.Map;

@FeignClient(
        value = "serpro-auth",
        url = "${serpro.url}",
        configuration = SerproAuthConfigClient.class
)
public interface SerproAuthClient {

    @PostMapping(value = "/token", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    ResponseEntity<TokenData> token(Map<String, ?> data);

}
