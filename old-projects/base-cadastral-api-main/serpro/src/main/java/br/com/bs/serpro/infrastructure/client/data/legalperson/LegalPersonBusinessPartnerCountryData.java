package br.com.bs.serpro.infrastructure.client.data.legalperson;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LegalPersonBusinessPartnerCountryData {

    @JsonProperty("codigo")
    private String code;

    @JsonProperty("descricao")
    private String description;

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}
