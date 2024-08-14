package br.com.bs.ppe.domain.entity;

import br.com.bs.ppe.domain.data.PPETypeEnum;
import br.com.bs.ppe.domain.entity.vo.NameVO;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class PPEEntity {

    private static final String FIELD_DOCUMENT = "cpf";
    private static final String FIELD_BIRTHDAY = "dataNascimento";
    private static final String FIELD_UNDERAGE_MESSAGE = "mensagemMenorIdade";
    private static final String FIELD_TYPE_CODE = "tipoPPE";
    private static final String FIELD_IS_TITLEHOLDER = "titular";
    private static final String FIELD_RELATIONSHIPS = "relacionados";
    private static final String FIELD_MANDATES = "mandatos";

    private final String document;

    private final String typeCode;

    private final NameVO name;

    private final List<RelationshipEntity> relationships;

    private final List<MandateEntity> mandates;

    public PPEEntity(String document, NameVO name) {
        this.document = document;
        this.typeCode = null;
        this.relationships = null;
        this.mandates = null;
        this.name = name;
    }

    public PPEEntity(String document, String typeCode, List<RelationshipEntity> relationships, List<MandateEntity> mandates) {
        this.document = document;
        this.name = null;
        this.typeCode = typeCode;
        this.relationships = relationships;
        this.mandates = mandates;
    }

    public PPEEntity(String document, String typeCode, NameVO name, List<RelationshipEntity> relationships, List<MandateEntity> mandates) {
        this.document = document;
        this.name = name;
        this.typeCode = typeCode;
        this.relationships = relationships;
        this.mandates = mandates;
    }

    public String getDocument() {
        return document;
    }

    public NameVO getName() {
        return name;
    }

    public Optional<NameVO> getOptionalName() {
        return Optional.ofNullable(name);
    }

    public String getTypeCode() {
        return typeCode;
    }

    public Boolean isTitleholder() {
        return PPETypeEnum.TITLEHOLDER.getCodeType().equals(this.typeCode);
    }

    public List<RelationshipEntity> getRelationships() {
        return relationships;
    }

    public List<MandateEntity> getMandates() {
        return mandates;
    }

    public PPEEntity withNameVO(NameVO nameVO) {
        return new PPEEntity(
                this.document,
                this.typeCode,
                nameVO,
                this.relationships,
                this.mandates
        );
    }

    public PPEEntity withRelationships(List<RelationshipEntity> relationships) {
        return new PPEEntity(
                this.document,
                this.typeCode,
                this.name,
                relationships,
                this.mandates
        );
    }

    private boolean shouldShowBirthdayInMap() {
        return this.getOptionalName()
                .map(it -> it.isUnder16YearsOld())
                .orElse(false);
    }

    public Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<>();
        map.put(FIELD_DOCUMENT, this.getDocument());
        this.getOptionalName()
                .map(it -> it.getUnderageMessage())
                .ifPresent(it -> map.put(FIELD_UNDERAGE_MESSAGE, it));
        if (this.shouldShowBirthdayInMap()) {
            map.put(FIELD_BIRTHDAY, this.getName().getBirthday());
        } else {
            map.put(FIELD_TYPE_CODE, this.getTypeCode());
            map.put(FIELD_IS_TITLEHOLDER, this.isTitleholder());
            map.put(FIELD_RELATIONSHIPS,
                    Optional.ofNullable(this.getRelationships())
                            .map(relationships -> relationships.stream()
                                    .map(it -> it.toMap())
                                    .collect(Collectors.toList())
                            )
                            .orElse(null)
            );
            map.put(FIELD_MANDATES,
                    Optional.ofNullable(this.getMandates())
                            .map(mandates -> mandates.stream()
                                    .map(it -> it.toMap())
                                    .collect(Collectors.toList())
                            )
                            .orElse(null)
            );
        }
        return map;
    }
}
