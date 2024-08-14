package br.com.bs.ppe.infrastructure.repository.bigtable.adapter.factories;

import br.com.bs.ppe.domain.entity.MandateEntity;
import br.com.bs.ppe.domain.entity.PPEEntity;
import br.com.bs.ppe.domain.entity.RelationshipEntity;
import br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel.PPEPessoasEntityModel;

import java.util.List;

public class PPEFactory {

    private PPEFactory() {

    }

    public static PPEEntity create(
            PPEPessoasEntityModel ppePessoasEntityModel,
            List<MandateEntity> mandates,
            List<RelationshipEntity> relationships
    ) {
        return new PPEEntity(
                ppePessoasEntityModel.getCpf(),
                ppePessoasEntityModel.getTypeCode(),
                relationships,
                mandates
        );
    }
}
