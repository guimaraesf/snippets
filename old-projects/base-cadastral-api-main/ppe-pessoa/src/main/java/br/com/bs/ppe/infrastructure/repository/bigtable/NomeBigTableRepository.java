package br.com.bs.ppe.infrastructure.repository.bigtable;

import br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel.NameEntityModel;

import java.util.List;
import java.util.Optional;

public interface NomeBigTableRepository {

    Optional<NameEntityModel> findByDocument(String document);

    List<NameEntityModel> findByDocumentList(List<String> documents);

}
