package br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel;

import java.util.Map;

public class NameEntityModel {

    public static final String CONTEXT = "nome";

    public static final String FIELD_DOCUMENT = "NUM_CPF_CNPJ";
    public static final String FIELD_BIRTHDAY = "DAT_NSC";

    private final String document;

    private final String birthday;


    public NameEntityModel(Map<String, String> data) {
        this(
                data.getOrDefault(FIELD_DOCUMENT, null),
                data.getOrDefault(FIELD_BIRTHDAY, null)
        );
    }

    public NameEntityModel(String document, String birthday) {
        this.document = document;
        this.birthday = birthday;
    }

    public String getDocument() {
        return document;
    }

    public String getBirthday() {
        return birthday;
    }

}
