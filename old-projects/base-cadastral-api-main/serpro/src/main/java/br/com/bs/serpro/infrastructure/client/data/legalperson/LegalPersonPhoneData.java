package br.com.bs.serpro.infrastructure.client.data.legalperson;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LegalPersonPhoneData {

    @JsonProperty("ddd")
    private String ddd;

    @JsonProperty("numero")
    private String number;

    public String getDdd() {
        return ddd;
    }

    public String getNumber() {
        return number;
    }
}
