package br.com.bs.ppe.infrastructure.repository.bigtable.adapter.factories;

import br.com.bs.ppe.domain.entity.MandateEntity;
import br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel.MandateEntityModel;
import br.com.bs.util.Util;

public class MandateFactory {

    private MandateFactory() {

    }

    public static MandateEntity create(MandateEntityModel mandateEntityModel) {
        return new MandateEntity(
                mandateEntityModel.getCpf(),
                mandateEntityModel.getPublicOffice(),
                mandateEntityModel.getPublicOfficeCode(),
                OrganizationFactory.create(mandateEntityModel),
                Util.toUSDate(mandateEntityModel.getInitialDate()),
                Util.toUSDate(mandateEntityModel.getEndDate())
        );
    }

}
