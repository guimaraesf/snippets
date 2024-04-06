package br.com.bs.interfaces;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface DatabasePpePessoaScan {
    Optional<List<Map<String, Object>>> ppe_pessoa(final String cpf);
    Optional<List<Map<String, Object>>> ppe_pessoa_nome(final String nome);
    Optional<List<Map<String, Object>>> ppe_pessoa_mandato(final String cpf);
    Optional<List<Map<String, Object>>> ppe_pessoa_relacionados(final String cpf);
    Optional<List<Map<String, Object>>> ppe_pessoa_titular(final String cpf);
    Optional<List<Map<String, Object>>> ppe_pessoa_titular_relacionado(final String cpfTitular, final String cpfRelacionado);
}
