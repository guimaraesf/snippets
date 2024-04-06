package br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel;

import java.util.Map;

public class OrganizationEntityModel {

    public static final String TABLE = "base_cadastral_ppe_descricao_orgao_produto";

    public static final String FIELD_ORGANIZATION_CODE = "COD_ORGAO";
    public static final String FIELD_ORGANIZATION_DESCRIPTION = "DESCRICAO_ORGAO";
    public static final String FIELD_HIGHER_ORGANIZATION_DESCRIPTION = "DESCRICAO_ORGAO_SUPERIOR";
    public static final String FIELD_WEBSITE = "SITE_ORGAO";
    public static final String FIELD_ESFERA_PODER_CODE = "CD_ES";
    public static final String FIELD_ESFERA_PODER_DESCRIPTION = "DESCRICAO_ESFERA_PODER";

    private final String code;

    private final String description;

    private final String higherOrganizationDescription;

    private final String website;

    private final String esferaPoderCode;

    private final String esferaPoderDescription;


    public OrganizationEntityModel(Map<String, String> data) {
        this(
                data.getOrDefault(FIELD_ORGANIZATION_CODE, null),
                data.getOrDefault(FIELD_ORGANIZATION_DESCRIPTION, null),
                data.getOrDefault(FIELD_HIGHER_ORGANIZATION_DESCRIPTION, null),
                data.getOrDefault(FIELD_WEBSITE, null),
                data.getOrDefault(FIELD_ESFERA_PODER_CODE, null),
                data.getOrDefault(FIELD_ESFERA_PODER_DESCRIPTION, null)
        );
    }

    public OrganizationEntityModel(
            String code,
            String description,
            String higherOrganizationDescription,
            String website,
            String esferaPoderCode,
            String esferaPoderDescription
    ) {
        this.code = code;
        this.description = description;
        this.higherOrganizationDescription = higherOrganizationDescription;
        this.website = website;
        this.esferaPoderCode = esferaPoderCode;
        this.esferaPoderDescription = esferaPoderDescription;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public String getHigherOrganizationDescription() {
        return higherOrganizationDescription;
    }

    public String getWebsite() {
        return website;
    }

    public String getEsferaPoderCode() {
        return esferaPoderCode;
    }

    public String getEsferaPoderDescription() {
        return esferaPoderDescription;
    }
}
