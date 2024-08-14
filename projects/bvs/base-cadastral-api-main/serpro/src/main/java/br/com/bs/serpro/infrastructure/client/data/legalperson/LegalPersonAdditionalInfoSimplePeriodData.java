package br.com.bs.serpro.infrastructure.client.data.legalperson;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LegalPersonAdditionalInfoSimplePeriodData {

    @JsonProperty("dataInicio")
    private String initData;

    @JsonProperty("dataFim")
    private String endDate;

    public String getInitData() {
        return initData;
    }

    public String getEndDate() {
        return endDate;
    }
}
