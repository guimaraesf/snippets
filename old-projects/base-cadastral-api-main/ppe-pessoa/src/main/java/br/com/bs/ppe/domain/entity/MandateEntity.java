package br.com.bs.ppe.domain.entity;

import br.com.bs.util.Util;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MandateEntity {

    private final static String FIELD_DOCUMENT = "cpf";
    private final static String FIELD_PUBLIC_OFFICE = "cargo";
    private final static String FIELD_PUBLIC_OFFICE_CODE = "codigoCargo";
    private final static String FIELD_ORGANIZATION = "orgao";
    private final static String FIELD_INITIAL_DATE = "dataInicio";
    private final static String FIELD_END_DATE = "dataFim";
    private final static String FIELD_IS_ACTIVE = "ativo";

    private final String document;

    private final String publicOffice;

    private final String publicOfficeCode;

    private final OrganizationEntity organization;

    private final LocalDate initialDate;

    private final LocalDate endDate;

    public MandateEntity(String document, String publicOffice, String publicOfficeCode, OrganizationEntity organization, LocalDate initialDate, LocalDate endDate) {
        this.document = document;
        this.publicOffice = publicOffice;
        this.publicOfficeCode = publicOfficeCode;
        this.organization = organization;
        this.initialDate = initialDate;
        this.endDate = endDate;
    }

    public String getDocument() {
        return document;
    }

    public String getPublicOffice() {
        return publicOffice;
    }

    public String getPublicOfficeCode() {
        return publicOfficeCode;
    }

    public OrganizationEntity getOrganization() {
        return organization;
    }

    public LocalDate getInitialDate() {
        return initialDate;
    }

    public LocalDate getEndDate() {
        return endDate;
    }

    public boolean isActive() {
        return this.endDate == null || this.endDate.isAfter(LocalDate.now());
    }

    public boolean isValidToShow() {
        return this.getEndDate() == null || LocalDate.now().minusYears(5).isBefore(this.getEndDate());
    }

    public MandateEntity withOrganization(OrganizationEntity organization) {
        return new MandateEntity(
                this.document,
                this.publicOffice,
                this.publicOfficeCode,
                Optional.ofNullable(organization).orElse(this.organization),
                this.initialDate,
                this.endDate
        );
    }

    public Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<>();
        map.put(FIELD_PUBLIC_OFFICE, this.getPublicOffice());
        map.put(FIELD_PUBLIC_OFFICE_CODE, this.getPublicOfficeCode());
        map.put(FIELD_ORGANIZATION, Optional.ofNullable(this.getOrganization()).map(it -> it.toMap()).orElse(null));
        map.put(FIELD_INITIAL_DATE, Util.toUSDate(this.getInitialDate()));
        map.put(FIELD_END_DATE, Util.toUSDate(this.getEndDate()));
        map.put(FIELD_IS_ACTIVE, this.isActive());
        return map;
    }
}
