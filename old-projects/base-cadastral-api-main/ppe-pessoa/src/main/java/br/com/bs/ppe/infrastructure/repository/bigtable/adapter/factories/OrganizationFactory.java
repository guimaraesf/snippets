package br.com.bs.ppe.infrastructure.repository.bigtable.adapter.factories;

import br.com.bs.ppe.domain.entity.OrganizationEntity;
import br.com.bs.ppe.infrastructure.repository.bigtable.adapter.factories.vo.EsferaPoderVOFactory;
import br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel.MandateEntityModel;
import br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel.OrganizationEntityModel;

public class OrganizationFactory {

    private OrganizationFactory() {

    }

    public static OrganizationEntity create(MandateEntityModel mandateEntityModel) {
        return new OrganizationEntity(
                mandateEntityModel.getOrganizationCode(),
                mandateEntityModel.getOrganizationName(),
                null,
                null,
                null,
                null
        );
    }

    public static OrganizationEntity create(OrganizationEntityModel organizationEntityModel) {
        return new OrganizationEntity(
                organizationEntityModel.getCode(),
                organizationEntityModel.getDescription(),
                organizationEntityModel.getHigherOrganizationDescription(),
                organizationEntityModel.getWebsite(),
                EsferaPoderVOFactory.create(organizationEntityModel),
                null
        );
    }
}
