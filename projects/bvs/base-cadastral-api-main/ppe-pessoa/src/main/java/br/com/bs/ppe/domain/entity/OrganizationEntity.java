package br.com.bs.ppe.domain.entity;

import br.com.bs.ppe.domain.entity.vo.EsferaPoderVO;
import br.com.bs.ppe.domain.entity.vo.OrganizationAddressVO;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class OrganizationEntity {

    private static final String FIELD_CODE = "codigo";
    private static final String FIELD_NAME = "nome";
    private static final String FIELD_HIGHER_ORGANIZATION_NAME = "nomeOrgaoSuperior";
    private static final String FIELD_WEBSITE = "website";
    private static final String FIELD_ESFERA_PODER = "esferaPoder";
    private static final String FIELD_ADDRESS = "endereco";

    private final String code;

    private final String name;

    private final String higherOrganizationName;

    private final String website;

    private final EsferaPoderVO esferaPoder;

    private final OrganizationAddressVO address;

    public OrganizationEntity(
            String code,
            String name,
            String higherOrganizationName,
            String website,
            EsferaPoderVO esferaPoder,
            OrganizationAddressVO address
    ) {
        this.code = code;
        this.name = name;
        this.higherOrganizationName = higherOrganizationName;
        this.website = website;
        this.esferaPoder = esferaPoder;
        this.address = address;
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public String getHigherOrganizationName() {
        return higherOrganizationName;
    }

    public String getWebsite() {
        return website;
    }

    public EsferaPoderVO getEsferaPoder() {
        return esferaPoder;
    }

    public OrganizationAddressVO getAddress() {
        return address;
    }

    public OrganizationEntity withAddress(OrganizationAddressVO address) {
        return new OrganizationEntity(
                this.code,
                this.name,
                this.higherOrganizationName,
                this.website,
                this.esferaPoder,
                Optional.ofNullable(address).orElse(this.address)
        );
    }

    public Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<>();
        map.put(FIELD_CODE, this.getCode());
        map.put(FIELD_NAME, this.getName());
        map.put(FIELD_HIGHER_ORGANIZATION_NAME, this.getHigherOrganizationName());
        map.put(FIELD_WEBSITE, this.getWebsite());
        map.put(FIELD_ESFERA_PODER, Optional.ofNullable(this.getEsferaPoder()).map(it -> it.toMap()).orElse(null));
        map.put(FIELD_ADDRESS, Optional.ofNullable(this.getAddress()).map(it -> it.toMap()).orElse(null));
        return map;
    }
}
