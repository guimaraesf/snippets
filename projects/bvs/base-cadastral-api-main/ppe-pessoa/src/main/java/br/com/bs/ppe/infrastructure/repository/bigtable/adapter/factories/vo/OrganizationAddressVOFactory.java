package br.com.bs.ppe.infrastructure.repository.bigtable.adapter.factories.vo;

import br.com.bs.ppe.domain.entity.vo.OrganizationAddressVO;
import br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel.OrganizationAddressEntityModel;

public class OrganizationAddressVOFactory {

    private OrganizationAddressVOFactory() {

    }

    public static OrganizationAddressVO create(OrganizationAddressEntityModel organizationAddressEntityModel) {
        return new OrganizationAddressVO(
                organizationAddressEntityModel.getOrganizationCode(),
                organizationAddressEntityModel.getMandateCode(),
                organizationAddressEntityModel.getAddressType(),
                organizationAddressEntityModel.getStreet(),
                organizationAddressEntityModel.getStreetNumber(),
                organizationAddressEntityModel.getComplement(),
                organizationAddressEntityModel.getCity(),
                organizationAddressEntityModel.getUf(),
                organizationAddressEntityModel.getZipCode()
        );
    }
}
