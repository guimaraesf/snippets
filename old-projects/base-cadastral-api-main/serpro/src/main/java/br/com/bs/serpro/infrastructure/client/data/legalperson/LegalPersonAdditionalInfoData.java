package br.com.bs.serpro.infrastructure.client.data.legalperson;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class LegalPersonAdditionalInfoData {

    @JsonProperty("optanteSimples")
    private String simpleOpting;

    @JsonProperty("optanteMei")
    private String meiOpting;

    @JsonProperty("listaPeriodoSimples")
    private List<LegalPersonAdditionalInfoSimplePeriodData> simplePeriods;

    public String getSimpleOpting() {
        return simpleOpting;
    }

    public String getMeiOpting() {
        return meiOpting;
    }

    public List<LegalPersonAdditionalInfoSimplePeriodData> getSimplePeriods() {
        return simplePeriods;
    }
}
