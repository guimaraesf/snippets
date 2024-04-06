package br.com.bs.serpro.infrastructure.client.data.legalperson;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LegalPersonSituationData {

    @JsonProperty("codigo")
    private String code;

    @JsonProperty("data")
    private String date;

    @JsonProperty("motivo")
    private String reason;

    public String getCode() {
        return code;
    }

    public String getDate() {
        return date;
    }

    public String getReason() {
        return reason;
    }
}
