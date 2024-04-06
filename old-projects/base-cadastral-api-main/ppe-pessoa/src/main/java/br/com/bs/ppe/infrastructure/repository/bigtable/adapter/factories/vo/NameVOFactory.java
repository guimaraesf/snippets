package br.com.bs.ppe.infrastructure.repository.bigtable.adapter.factories.vo;

import br.com.bs.ppe.domain.entity.vo.NameVO;
import br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel.NameEntityModel;

import java.util.Optional;

public class NameVOFactory {

    private NameVOFactory() {

    }

    public static NameVO create(NameEntityModel nameEntityModel) {
        return new NameVO(
                cleanDocument(nameEntityModel.getDocument()),
                nameEntityModel.getBirthday()
        );
    }

    private static String cleanDocument(String document) {
        return Optional.ofNullable(document)
                .map(it -> {
                    if (it.length() == 14) {
                        return it.substring(3);
                    }
                    return it;
                }).orElse(document);
    }
}
