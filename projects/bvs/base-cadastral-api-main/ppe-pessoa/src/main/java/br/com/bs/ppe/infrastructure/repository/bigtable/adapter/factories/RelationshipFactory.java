package br.com.bs.ppe.infrastructure.repository.bigtable.adapter.factories;

import br.com.bs.ppe.domain.entity.RelationshipEntity;
import br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel.RelationshipEntityModel;

public class RelationshipFactory {

    private RelationshipFactory() {

    }

    public static RelationshipEntity create(RelationshipEntityModel relationshipEntityModel) {
        return new RelationshipEntity(
                relationshipEntityModel.getCpfRelationship(),
                relationshipEntityModel.getCpfTitleholder(),
                relationshipEntityModel.getRelationshipType()
        );
    }
}
