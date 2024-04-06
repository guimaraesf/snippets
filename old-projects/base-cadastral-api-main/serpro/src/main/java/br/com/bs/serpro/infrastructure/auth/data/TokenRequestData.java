package br.com.bs.serpro.infrastructure.auth.data;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TokenRequestData {

    @JsonProperty("grant_type")
    private final String grantType = "client_credentials";


}
