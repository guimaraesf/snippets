package br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel;

import java.util.Map;

public class MandateEntityModel {

    public static final String TABLE = "base_cadastral_ppe_mandatos_produto";
    public static final String FIELD_CPF = "NUM_CPF";
    public static final String FIELD_INITIAL_DATE = "DAT_INI_ATV";
    public static final String FIELD_END_DATE = "DAT_FIM_ATV";
    public static final String FIELD_ESFERA_PODER = "DSC_PPE_ESF_PDR";
    public static final String FIELD_ORGANIZATION_CODE = "COD_PPE_ORG";
    public static final String FIELD_ORGANIZATION_NAME = "DSC_PPE_ORG";
    public static final String FIELD_PUBLIC_OFFICE = "DSC_PPE_CRG";
    public static final String FIELD_PUBLIC_OFFICE_CODE = "COD_PPE_CRG";

    private final String cpf;

    private final String initialDate;

    private final String endDate;

    private final String esferaPoder;

    private final String organizationCode;

    private final String organizationName;

    private final String publicOffice;

    private final String publicOfficeCode;

    public MandateEntityModel(Map<String, String> data){
        this(
                data.getOrDefault(FIELD_CPF, null),
                data.getOrDefault(FIELD_INITIAL_DATE, null),
                data.getOrDefault(FIELD_END_DATE, null),
                data.getOrDefault(FIELD_ESFERA_PODER, null),
                data.getOrDefault(FIELD_ORGANIZATION_CODE, null),
                data.getOrDefault(FIELD_ORGANIZATION_NAME, null),
                data.getOrDefault(FIELD_PUBLIC_OFFICE, null),
                data.getOrDefault(FIELD_PUBLIC_OFFICE_CODE, null)
        );
    }

    public MandateEntityModel(
            String cpf,
            String initialDate,
            String endDate,
            String esferaPoder,
            String organizationCode,
            String organizationName,
            String publicOffice,
            String publicOfficeCode
    ) {
        this.cpf = cpf;
        this.initialDate = initialDate;
        this.endDate = endDate;
        this.esferaPoder = esferaPoder;
        this.organizationCode = organizationCode;
        this.organizationName = organizationName;
        this.publicOffice = publicOffice;
        this.publicOfficeCode = publicOfficeCode;
    }

    public String getCpf() {
        return cpf;
    }

    public String getInitialDate() {
        return initialDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public String getEsferaPoder() {
        return esferaPoder;
    }

    public String getOrganizationCode() {
        return organizationCode;
    }

    public String getOrganizationName() {
        return organizationName;
    }

    public String getPublicOffice() {
        return publicOffice;
    }

    public String getPublicOfficeCode() {
        return publicOfficeCode;
    }
}
