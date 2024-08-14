package br.com.bs.serpro.infrastructure.client.data.legalperson;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LegalPersonBusinessPartnerData {

    @JsonProperty("tipoSocio")
    private String type;

    @JsonProperty("cpf")
    private String cpf;

    @JsonProperty("nome")
    private String name;

    @JsonProperty("qualificacao")
    private String qualification;

    @JsonProperty("dataInclusao")
    private String inclusionDate;

    @JsonProperty("pais")
    private LegalPersonBusinessPartnerCountryData country;

    @JsonProperty("representanteLegal")
    private LegalPersonBusinessPartnerLegalRepresentativeData legalRepresentative;

    public String getType() {
        return type;
    }

    public String getCpf() {
        return cpf;
    }

    public String getName() {
        return name;
    }

    public String getQualification() {
        return qualification;
    }

    public String getInclusionDate() {
        return inclusionDate;
    }

    public LegalPersonBusinessPartnerCountryData getCountry() {
        return country;
    }

    public LegalPersonBusinessPartnerLegalRepresentativeData getLegalRepresentative() {
        return legalRepresentative;
    }
}
