package br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel;

import java.util.Map;

public class OrganizationAddressEntityModel {

    public static final String TABLE = "base_cadastral_ppe_endereco_orgao_produto";

    public static final String FIELD_CODE = "COD_CARGO_ORGAO";
    public static final String FIELD_ADDRESS_TYPE = "TIP_LOGRADOURO_ORGAO";
    public static final String FIELD_STREET = "LOGRADOURO_ORGAO";
    public static final String FIELD_STREET_NUMBER = "NR_LOGRAD";
    public static final String FIELD_STREET_COMPLEMENT = "COMPLEM_LOGRAD_ORGAO";
    public static final String FIELD_CITY = "CIDADE_ORGAO";
    public static final String FIELD_UF = "UF";
    public static final String FIELD_ZIP_CODE = "CEP_ORG";

    private final String code;

    private final String organizationCode;

    private final String mandateCode;

    private final String addressType;

    private final String street;

    private final String streetNumber;

    private final String complement;

    private final String city;

    private final String uf;

    private final String zipCode;


    public OrganizationAddressEntityModel(String organizationCode, String mandateCode, Map<String, String> data) {
        this(
                data.getOrDefault(FIELD_CODE, null),
                organizationCode,
                mandateCode,
                data.getOrDefault(FIELD_ADDRESS_TYPE, null),
                data.getOrDefault(FIELD_STREET, null),
                data.getOrDefault(FIELD_STREET_NUMBER, null),
                data.getOrDefault(FIELD_STREET_COMPLEMENT, null),
                data.getOrDefault(FIELD_CITY, null),
                data.getOrDefault(FIELD_UF, null),
                data.getOrDefault(FIELD_ZIP_CODE, null)
        );
    }

    public OrganizationAddressEntityModel(
            String code,
            String organizationCode,
            String mandateCode,
            String addressType,
            String street,
            String streetNumber,
            String complement,
            String city,
            String uf,
            String zipCode
    ) {
        this.code = code;
        this.organizationCode = organizationCode;
        this.mandateCode = mandateCode;
        this.addressType = addressType;
        this.street = street;
        this.streetNumber = streetNumber;
        this.complement = complement;
        this.city = city;
        this.uf = uf;
        this.zipCode = zipCode;
    }

    public String getCode() {
        return code;
    }

    public String getOrganizationCode() {
        return organizationCode;
    }

    public String getMandateCode() {
        return mandateCode;
    }

    public String getAddressType() {
        return addressType;
    }

    public String getStreet() {
        return street;
    }

    public String getStreetNumber() {
        return streetNumber;
    }

    public String getComplement() {
        return complement;
    }

    public String getCity() {
        return city;
    }

    public String getUf() {
        return uf;
    }

    public String getZipCode() {
        return zipCode;
    }
}
