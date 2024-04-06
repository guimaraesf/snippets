package br.com.bs.serpro.infrastructure.client.data.physicalperson;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PhysicalPersonData {

    @JsonProperty("ni")
    private String ni;

    @JsonProperty("nome")
    private String name;

    @JsonProperty("situacao")
    private PhysicalPersonSituationData situation;

    @JsonProperty("nascimento")
    private String birthday;

    @JsonProperty("obito")
    private String deathYear;

    public String getNi() {
        return ni;
    }

    public String getName() {
        return name;
    }

    public PhysicalPersonSituationData getSituation() {
        return situation;
    }

    public String getBirthday() {
        return birthday;
    }

    public String getDeathYear() {
        return deathYear;
    }

}
