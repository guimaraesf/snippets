package br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel;

import java.util.Map;

public class PPEPessoasEntityModel {

    public static final String TABLE = "base_cadastral_ppe_pessoas_produto";
    public static final String FIELD_CPF = "NUM_CPF";
    public static final String FIELD_NAME = "NOM_PPE";
    public static final String FIELD_BIRTHDAY = "DAT_NSC";
    public static final String FIELD_TYPE_CODE = "COD_TPO_PPE";

    private final String cpf;

    private final String name;

    private final String birthday;

    private final String typeCode;

    public PPEPessoasEntityModel(Map<String, String> data){
        this(
                data.getOrDefault(FIELD_CPF, null),
                data.getOrDefault(FIELD_NAME, null),
                data.getOrDefault(FIELD_BIRTHDAY, null),
                data.getOrDefault(FIELD_TYPE_CODE, null)
        );
    }

    public PPEPessoasEntityModel(String cpf, String name, String birthday, String typeCode) {
        this.cpf = cpf;
        this.name = name;
        this.birthday = birthday;
        this.typeCode = typeCode;
    }

    public String getCpf() {
        return cpf;
    }

    public String getName() {
        return name;
    }

    public String getBirthday() {
        return birthday;
    }

    public String getTypeCode() {
        return typeCode;
    }
}
