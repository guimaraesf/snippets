package br.com.bs.serpro.application.service;

import br.com.bs.serpro.infrastructure.client.data.legalperson.LegalPersonData;
import br.com.bs.serpro.infrastructure.client.data.physicalperson.PhysicalPersonData;

import java.util.Optional;

public interface SerproSearchService {

    Optional<PhysicalPersonData> searchPhysicalPerson(String cpf, boolean onlyFromCache);

    Optional<LegalPersonData> searchLegalPerson(String cnpj);

}
