package br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel;

import java.util.Map;

public class RelationshipEntityModel {

    public static final String TABLE = "base_cadastral_ppe_relacionados_produto";
    public static final String FAMILY_TITLEHOLDER = "titular";
    public static final String FAMILY_RELATIONSHIP = "relacionados";

    public static final String FIELD_CPF_TITLEHOLDER = "NUM_CPF_TIT";
    public static final String FIELD_CPF_RELATIONSHIP = "NUM_CPF_REL";
    public static final String FIELD_RELATIONSHIP_TYPE = "DSC_TPO_REL";

    private final String cpfTitleholder;

    private final String cpfRelationship;

    private final String relationshipType;


    public RelationshipEntityModel(Map<String, String> data) {
        this(
                data.getOrDefault(FIELD_CPF_TITLEHOLDER, null),
                data.getOrDefault(FIELD_CPF_RELATIONSHIP, null),
                data.getOrDefault(FIELD_RELATIONSHIP_TYPE, null)
        );
    }

    public RelationshipEntityModel(String cpfTitleholder, String cpfRelationship, String relationshipType) {
        this.cpfTitleholder = cpfTitleholder;
        this.cpfRelationship = cpfRelationship;
        this.relationshipType = relationshipType;
    }

    public String getCpfTitleholder() {
        return cpfTitleholder;
    }

    public String getCpfRelationship() {
        return cpfRelationship;
    }

    public String getRelationshipType() {
        return relationshipType;
    }
}
