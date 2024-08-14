package br.com.bs.ppe.domain.entity;

import br.com.bs.util.Util;

import java.util.HashMap;
import java.util.Map;

public class RelationshipEntity {

    private static final String FIELD_DOCUMENT = "cpf";
    private static final String FIELD_DOCUMENT_TITLEHOLDER = "cpfTitular";
    private static final String FIELD_RELATION_TYPE = "tipoRelacionamento";

    private final String document;

    private final String documentTitleholder;

    private final String relationType;

    public RelationshipEntity(String document, String documentTitleholder, String relationType) {
        this.document = document;
        this.documentTitleholder = documentTitleholder;
        this.relationType = Util.cleanStringOrNull(relationType);
    }

    public String getDocument() {
        return document;
    }

    public String getDocumentTitleholder() {
        return documentTitleholder;
    }

    public String getRelationType() {
        return relationType;
    }

    public Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<>();
        map.put(FIELD_DOCUMENT, this.getDocument());
        map.put(FIELD_DOCUMENT_TITLEHOLDER, this.getDocumentTitleholder());
        map.put(FIELD_RELATION_TYPE, this.getRelationType());
        return map;
    }
}
