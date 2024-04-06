package br.com.bs.interfaces;

import java.util.List;
import java.util.Optional;

public interface DatabaseReversedIndexScan {
    Optional<List<String>> findReverdIndexTable(final String origin, final String verbo);
    Optional<List<String>> findReverdIndexTable(final String origin, final String verbo, final String destino);
}
