package br.com.bs.ppe.domain.repository;

import br.com.bs.ppe.domain.entity.PPEEntity;

import java.util.Optional;

public interface PPERepository {

    Optional<PPEEntity> findByDocument(String document);

}
