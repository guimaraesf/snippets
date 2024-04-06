package br.com.bs.serpro.infrastructure.client.data.legalperson;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LegalPersonAddressData {

    @JsonProperty("tipoLogradouro")
    private String typeAddress;

    @JsonProperty("logradouro")
    private String address;

    @JsonProperty("numero")
    private String number;

    @JsonProperty("complemento")
    private String complement;

    @JsonProperty("cep")
    private String zipCode;

    @JsonProperty("bairro")
    private String neighborhood;

    @JsonProperty("municipio")
    private LegalPersonAddressCityData city;

    @JsonProperty("uf")
    private String state;

    @JsonProperty("pais")
    private LegalPersonAddressCountryData country;

    public String getTypeAddress() {
        return typeAddress;
    }

    public String getAddress() {
        return address;
    }

    public String getNumber() {
        return number;
    }

    public String getComplement() {
        return complement;
    }

    public String getZipCode() {
        return zipCode;
    }

    public String getNeighborhood() {
        return neighborhood;
    }

    public LegalPersonAddressCityData getCity() {
        return city;
    }

    public String getState() {
        return state;
    }

    public LegalPersonAddressCountryData getCountry() {
        return country;
    }
}
