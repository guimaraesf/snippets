package br.com.bs.serpro.infrastructure.auth.data;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TokenData {

    private String scope;

    @JsonProperty("token_type")
    private String tokenType;

    @JsonProperty("expires_in")
    private Integer expiresIn;

    @JsonProperty("access_token")
    private String accessToken;

    public String getScope() {
        return scope;
    }

    public String getTokenType() {
        return tokenType;
    }

    public Integer getExpiresIn() {
        return expiresIn;
    }

    public String getAccessToken() {
        return accessToken;
    }
}
