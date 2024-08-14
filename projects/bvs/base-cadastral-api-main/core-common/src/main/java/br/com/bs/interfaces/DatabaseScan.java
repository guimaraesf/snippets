package br.com.bs.interfaces;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface DatabaseScan {

    Optional<Map<String, Object>> findProdutoTable(final String contexto, final Map<String, String> requestMap, String...fetchColumns);
    Optional<Map<String, Object>> findProdutoTable(final String contexto, final Map<String, String> requestMap, boolean inibir, String...fetchColumns);

    Optional<List<Map<String, Object>>> findFullProdutoTableList(final String contexto, final Map<String, String> requestMap, String...fetchColumns);

    Optional<List<Map<String, Object>>> findProdutoTableList(final String contexto, final Map<String, String> requestMap, String...fetchColumns);
    Optional<List<Map<String, Object>>> findProdutoTableList(final String contexto, final Map<String, String> requestMap, boolean inibir, String...fetchColumns);
}