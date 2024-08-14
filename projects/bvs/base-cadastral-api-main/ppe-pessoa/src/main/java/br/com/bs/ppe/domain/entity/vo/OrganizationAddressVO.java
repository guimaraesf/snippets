package br.com.bs.ppe.domain.entity.vo;

import java.util.HashMap;
import java.util.Map;

public class OrganizationAddressVO {

    private static final String FIELD_ADDRESS_TYPE = "tipoLogradouro";
    private static final String FIELD_STREET = "logradouro";
    private static final String FIELD_STREET_NUMBER = "numero";
    private static final String FIELD_STREET_COMPLEMENT = "complemento";
    private static final String FIELD_CITY = "cidade";
    private static final String FIELD_UF = "uf";
    private static final String FIELD_ZIP_CODE = "cep";

    private final String organizationCode;

    private final String mandateCode;

    private final String addressType;

    private final String street;

    private final String streetNumber;

    private final String complement;

    private final String city;

    private final String uf;

    private final String zipCode;


    public OrganizationAddressVO(
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

    public Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<>();
        map.put(FIELD_ADDRESS_TYPE, this.getAddressType());
        map.put(FIELD_STREET, this.getStreet());
        map.put(FIELD_STREET_NUMBER, this.getStreetNumber());
        map.put(FIELD_STREET_COMPLEMENT, this.getComplement());
        map.put(FIELD_CITY, this.getCity());
        map.put(FIELD_UF, this.getUf());
        map.put(FIELD_ZIP_CODE, this.getZipCode());
        return map;
    }
}
