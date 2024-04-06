package br.com.bs.interfaces;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface DatabaseOutrasGrafiasScan {
    Optional<List<Map<String, Object>>> outras_grafias(final String cpf);
}
