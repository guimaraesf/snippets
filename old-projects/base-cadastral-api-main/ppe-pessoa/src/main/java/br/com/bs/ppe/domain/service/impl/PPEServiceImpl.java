package br.com.bs.ppe.domain.service.impl;

import br.com.bs.ppe.domain.entity.PPEEntity;
import br.com.bs.ppe.domain.entity.RelationshipEntity;
import br.com.bs.ppe.domain.entity.vo.NameVO;
import br.com.bs.ppe.domain.repository.PPERepository;
import br.com.bs.ppe.domain.service.PPEService;
import br.com.bs.ppe.infrastructure.repository.bigtable.NomeBigTableRepository;
import br.com.bs.ppe.infrastructure.repository.bigtable.adapter.factories.vo.NameVOFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class PPEServiceImpl implements PPEService {

    private final PPERepository ppeRepository;

    private final NomeBigTableRepository nomeBigTableRepository;

    public PPEServiceImpl(PPERepository ppeRepository, NomeBigTableRepository nomeBigTableRepository) {
        this.ppeRepository = ppeRepository;
        this.nomeBigTableRepository = nomeBigTableRepository;
    }

    @Override
    public Optional<PPEEntity> findByCPF(String cpf) {
        return this.nomeBigTableRepository.findByDocument(cpf)
                .map(it -> NameVOFactory.create(it))
                .flatMap(nome -> {
                    if (nome.isUnder16YearsOld()) {
                        return Optional.of(new PPEEntity(cpf, nome));
                    }
                    return this.ppeRepository.findByDocument(cpf)
                            .map(it -> it.withNameVO(nome));
                })
                .map(ppe -> this.cleanRelationships(ppe));
    }

    private PPEEntity cleanRelationships(PPEEntity ppe){
        if (ppe.getRelationships() == null || ppe.getRelationships().isEmpty()) {
            return ppe;
        }
        List<String> relationshipDocuments = ppe.getRelationships()
                .stream()
                .map(it -> ppe.isTitleholder() ? it.getDocument() : it.getDocumentTitleholder())
                .distinct()
                .collect(Collectors.toList());

        List<NameVO> names = this.nomeBigTableRepository.findByDocumentList(relationshipDocuments)
                .stream()
                .map(it -> NameVOFactory.create(it))
                .filter(it -> !it.isUnderage() || StringUtils.isEmpty(it.getBirthday()))
                .collect(Collectors.toList());

        List<RelationshipEntity> relationships = ppe.getRelationships()
                .stream()
                .filter(relationship ->
                        names.stream()
                                .anyMatch(it ->
                                        it.getDocument().equals(
                                                ppe.isTitleholder() ? relationship.getDocument() : relationship.getDocumentTitleholder()
                                        )
                                )
                )
                .collect(Collectors.toList());

        return ppe.withRelationships(relationships);
    }

}
