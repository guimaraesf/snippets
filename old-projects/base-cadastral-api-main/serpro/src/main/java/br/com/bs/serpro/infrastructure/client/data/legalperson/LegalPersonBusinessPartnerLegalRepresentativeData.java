package br.com.bs.serpro.infrastructure.client.data.legalperson;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LegalPersonBusinessPartnerLegalRepresentativeData {

    @JsonProperty("cpf")
    private String cpf;

    @JsonProperty("nome")
    private String name;

    @JsonProperty("qualificacao")
    private String qualification;

    public String getCpf() {
        return cpf;
    }

    public String getName() {
        return name;
    }

    public String getQualification() {
        return qualification;
    }
}
