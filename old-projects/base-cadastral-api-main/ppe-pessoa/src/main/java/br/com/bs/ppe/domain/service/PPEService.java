package br.com.bs.ppe.domain.service;

import br.com.bs.ppe.domain.entity.PPEEntity;

import java.util.Optional;

public interface PPEService {

    Optional<PPEEntity> findByCPF(String cpf);

}
