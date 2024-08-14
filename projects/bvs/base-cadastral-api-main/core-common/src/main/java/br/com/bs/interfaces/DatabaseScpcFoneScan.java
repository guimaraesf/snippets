package br.com.bs.interfaces;

import java.util.List;
import java.util.Optional;
import java.util.Map;

public interface DatabaseScpcFoneScan {
    Optional<List<Map<String, Object>>> findScpcFonTable(final String telefone);
}
