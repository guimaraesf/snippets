package br.com.bs.serpro.infrastructure.client.data.legalperson;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class LegalPersonData {

    @JsonProperty("ni")
    private String ni;

    @JsonProperty("tipoEstabelecimento")
    private String establishmentType;

    @JsonProperty("nomeEmpresarial")
    private String companyName;

    @JsonProperty("nomeFantasia")
    private String tradeName;

    @JsonProperty("situacaoCadastral")
    private LegalPersonSituationData situation;

    @JsonProperty("naturezaJuridica")
    private LegalPersonNatureData legalNature;

    @JsonProperty("dataAbertura")
    private String openingDate;

    @JsonProperty("cnaePrincipal")
    private LegalPersonCnaeData primaryCnae;

    @JsonProperty("cnaeSecundarias")
    private List<LegalPersonCnaeData> secondariesCnae;

    @JsonProperty("endereco")
    private LegalPersonAddressData address;

    @JsonProperty("municipioJurisdicao")
    private LegalPersonCityJurisdictionData countyJurisdiction;

    @JsonProperty("telefones")
    private List<LegalPersonPhoneData> phones;

    @JsonProperty("correioEletronico")
    private String email;

    @JsonProperty("capitalSocial")
    private String shareCapital;

    @JsonProperty("porte")
    private String size;

    @JsonProperty("situacaoEspecial")
    private String especialSituation;

    @JsonProperty("dataSituacaoEspecial")
    private String especialSituationDate;

    @JsonProperty("informacoesAdicionais")
    private LegalPersonAdditionalInfoData additionalInformation;

    @JsonProperty("socios")
    private List<LegalPersonBusinessPartnerData> partners;

    public String getNi() {
        return ni;
    }

    public String getEstablishmentType() {
        return establishmentType;
    }

    public String getCompanyName() {
        return companyName;
    }

    public String getTradeName() {
        return tradeName;
    }

    public LegalPersonSituationData getSituation() {
        return situation;
    }

    public LegalPersonNatureData getLegalNature() {
        return legalNature;
    }

    public String getOpeningDate() {
        return openingDate;
    }

    public LegalPersonCnaeData getPrimaryCnae() {
        return primaryCnae;
    }

    public List<LegalPersonCnaeData> getSecondariesCnae() {
        return secondariesCnae;
    }

    public LegalPersonAddressData getAddress() {
        return address;
    }

    public LegalPersonCityJurisdictionData getCountyJurisdiction() {
        return countyJurisdiction;
    }

    public List<LegalPersonPhoneData> getPhones() {
        return phones;
    }

    public String getEmail() {
        return email;
    }

    public String getShareCapital() {
        return shareCapital;
    }

    public String getSize() {
        return size;
    }

    public String getEspecialSituation() {
        return especialSituation;
    }

    public String getEspecialSituationDate() {
        return especialSituationDate;
    }

    public LegalPersonAdditionalInfoData getAdditionalInformation() {
        return additionalInformation;
    }

    public List<LegalPersonBusinessPartnerData> getPartners() {
        return partners;
    }
}
