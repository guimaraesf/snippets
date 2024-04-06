package br.com.bs.serpro.application.service.data;

import br.com.bs.serpro.infrastructure.client.data.legalperson.LegalPersonData;
import br.com.bs.serpro.infrastructure.client.data.physicalperson.PhysicalPersonData;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;
import java.util.Optional;

public class SerproSearchData {

    @JsonProperty("ni")
    private String ni;

    @JsonProperty("physicalPerson")
    private PhysicalPersonData physicalPerson;

    @JsonProperty("legalPerson")
    private LegalPersonData legalPerson;

    @JsonProperty("searchDate")
    private LocalDateTime searchDate;

    @JsonProperty("isUnderage")
    private Boolean isUnderage;

    @JsonProperty("isNotFound")
    private Boolean isNotFound;

    @JsonCreator
    public SerproSearchData(
            String ni,
            PhysicalPersonData physicalPerson,
            LegalPersonData legalPerson,
            LocalDateTime searchDate,
            Boolean isUnderage,
            Boolean isNotFound
    ) {
        this.ni = ni;
        this.physicalPerson = physicalPerson;
        this.legalPerson = legalPerson;
        this.searchDate = searchDate;
        this.isUnderage = Optional.ofNullable(isUnderage).orElse(false);
        this.isNotFound = Optional.ofNullable(isNotFound).orElse(false);
    }

    public SerproSearchData(PhysicalPersonData physicalPerson, LocalDateTime searchDate) {
        this.ni = physicalPerson.getNi();
        this.physicalPerson = physicalPerson;
        this.searchDate = searchDate;
        this.isUnderage = false;
        this.isNotFound = false;
    }

    public SerproSearchData(LegalPersonData legalPerson, LocalDateTime searchDate) {
        this.ni = legalPerson.getNi();
        this.legalPerson = legalPerson;
        this.searchDate = searchDate;
        this.isUnderage = false;
        this.isNotFound = false;
    }

    public PhysicalPersonData getPhysicalPerson() {
        return physicalPerson;
    }

    public LegalPersonData getLegalPerson() {
        return legalPerson;
    }

    public LocalDateTime getSearchDate() {
        return searchDate;
    }

    public Boolean getIsUnderage() {
        return isUnderage;
    }

    public Boolean getIsNotFound() {
        return isNotFound;
    }

    public static SerproSearchData create(PhysicalPersonData physicalPerson) {
        return new SerproSearchData(physicalPerson, LocalDateTime.now());
    }

    public static SerproSearchData create(LegalPersonData legalPerson) {
        return new SerproSearchData(legalPerson, LocalDateTime.now());
    }

    public static SerproSearchData createUnderage(String ni) {
        return new SerproSearchData(
                ni,
                null,
                null,
                LocalDateTime.now(),
                true,
                false
        );
    }

    public static SerproSearchData createNotFound(String ni) {
        return new SerproSearchData(
                ni,
                null,
                null,
                LocalDateTime.now(),
                false,
                true
        );
    }
}
