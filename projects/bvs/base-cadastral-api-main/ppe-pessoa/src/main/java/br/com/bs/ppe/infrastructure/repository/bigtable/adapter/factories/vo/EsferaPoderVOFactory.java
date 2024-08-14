package br.com.bs.ppe.infrastructure.repository.bigtable.adapter.factories.vo;

import br.com.bs.ppe.domain.entity.vo.EsferaPoderVO;
import br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel.OrganizationEntityModel;

public class EsferaPoderVOFactory {

    private EsferaPoderVOFactory() {

    }

    public static EsferaPoderVO create(OrganizationEntityModel organizationEntityModel) {
        return new EsferaPoderVO(
                organizationEntityModel.getEsferaPoderCode(),
                organizationEntityModel.getEsferaPoderDescription()
        );
    }
}
