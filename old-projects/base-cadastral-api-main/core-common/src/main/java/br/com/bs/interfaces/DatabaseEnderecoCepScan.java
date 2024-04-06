package br.com.bs.interfaces;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface DatabaseEnderecoCepScan {
    Optional<List<Map<String, Object>>> endereco_cep(final String cep);
}
