package br.com.bs.bigtable;

import br.com.bs.bigtable.config.BigTableConfig;
import br.com.bs.config.AppConfig;
import br.com.bs.config.Constants;
import br.com.bs.exception.ApiTimeoutException;
import br.com.bs.interfaces.DatabaseScan;
import br.com.bs.serpro.application.service.SerproSearchService;
import br.com.bs.serpro.domain.converter.ContextConverter;
import br.com.bs.serpro.infrastructure.client.data.legalperson.LegalPersonData;
import br.com.bs.serpro.infrastructure.client.data.physicalperson.PhysicalPersonData;
import br.com.bs.serpro.infrastructure.client.exception.SerproDocumentNotFoundException;
import br.com.bs.serpro.infrastructure.client.exception.SerproUnderageException;
import br.com.bs.util.Util;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static br.com.bs.util.Util.md5Hash;
import static br.com.bs.util.Util.printLogger;
import static java.util.Optional.of;


@Profile("bigtable")
@Service
public class BigTableScan implements DatabaseScan {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private final BigTableConfig bigTableConfig;
    private final AppConfig config;
    private final SerproSearchService serproSearchService;
    private final List<ContextConverter<?>> contextConverters;

    private static final List<String> SERPRO_PHYSICAL_PERSON_CONTEXTS = Arrays.asList(
            Constants.C_NOME,
            Constants.C_OBITO,
            Constants.C_ANO_OBITO,
            Constants.C_PROTOCOLO
    );

    private static final List<String> SERPRO_PHYSICAL_PERSON_CONTEXTS_ONLY_CACHE = Arrays.asList(
            Constants.C_OBITO,
            Constants.C_ANO_OBITO,
            Constants.C_PROTOCOLO
    );

    private static final List<String> SERPRO_PHYSICAL_PERSON_REQUEST_CONTEXTS_ONLY_TRIGGER = Arrays.asList(
            Constants.C_NOME,
            "v1-identificacao",
            "socket-identificacao"
    );

    private static final List<String> SERPRO_LEGAL_PERSON_CONTEXTS = Arrays.asList(
            "opcao_mei",
            "receita_pj",
            "ccm"
    );

    private static final List<String> SERPRO_CONTEXTS = Stream.concat(
            SERPRO_PHYSICAL_PERSON_CONTEXTS.stream(),
            SERPRO_LEGAL_PERSON_CONTEXTS.stream()
    ).collect(Collectors.toList());

    @Autowired
    public BigTableScan(
            BigTableConfig bigTableConfig,
            AppConfig config,
            SerproSearchService serproSearchService, List<ContextConverter<?>> contextConverters) {
        this.bigTableConfig = bigTableConfig;
        this.config = config;
        this.serproSearchService = serproSearchService;
        this.contextConverters = contextConverters;
    }

    @Override
    public Optional<Map<String, Object>> findProdutoTable(final String contexto, final Map<String, String> requestMap, String... fetchColumns) {
        return this.findProdutoTable(contexto, requestMap, true, fetchColumns);
    }

    @Override
    public Optional<Map<String, Object>> findProdutoTable(final String contexto, final Map<String, String> requestMap, boolean inibir, String... fetchColumns) {
        return scanTable(contexto, requestMap, inibir, 1, fetchColumns).stream().findFirst();
    }

    @Override
    public Optional<List<Map<String, Object>>> findProdutoTableList(final String contexto, final Map<String, String> requestMap, String... fetchColumns) {
        return this.findProdutoTableList(contexto, requestMap, true, fetchColumns);
    }

    @Override
    public Optional<List<Map<String, Object>>> findFullProdutoTableList(final String contexto, final Map<String, String> requestMap, String... fetchColumns) {
        return of(scanTable(contexto, requestMap, false, 0, fetchColumns));
    }

    @Override
    public Optional<List<Map<String, Object>>> findProdutoTableList(final String contexto, final Map<String, String> requestMap, boolean inibir, String... fetchColumns) {
        return of(scanTable(contexto, requestMap, inibir, /*0*/ bigTableConfig.getLimitRows(), fetchColumns));
    }

    private List<Map<String, Object>> scanTable(String contexto, Map<String, String> requestMap, boolean inibir, int limit, String... fetchColumns) {
        final Instant started = Instant.now();
        final List<Map<String, Object>> result = new ArrayList<>(); //Resultados da consulta
        Map<String, Object> row = new HashMap<>(); //Map da linha
        String cpf = requestMap.get("cpf");
        final String originalCPF = cpf;
        String tableName = null;
        String requestContext = requestMap.getOrDefault("contexto", null);
        boolean isInhibitedForSerpro = false;
        final String contextoOriginal = contexto;
        if (Constants.C_OBITO.equals(contextoOriginal)) {
            contexto = Constants.C_ANO_OBITO;
        }
        if (Constants.C_PPE.equals(contextoOriginal)) {
            contexto = "ppe_pessoas";
            cpf = cpf.length() == 14 ? cpf.substring(3) : cpf;
        }
        try {
            tableName = bigTableConfig.getTableName(contexto, bigTableConfig.getTableProduto());
            long timestamp = 0;

            for (int i = 0; i < limit; i++) {
                String cpfhash = Util.md5Hash(cpf) + "-" + String.valueOf(i);
                Row r = this.bigTableConfig.buildConnection().readRow(tableName, cpfhash);
                if (r == null) {
                    continue;
                }
                row = new HashMap<>();
                boolean inibicao = false;
                timestamp = buscaRecente(r);
                for (RowCell cell : r.getCells()) {
                    String column = cell.getQualifier().toStringUtf8();
                    String value = cell.getValue().toStringUtf8();

                    if (inibir == true) {
                        if (column.trim().toUpperCase().equals(config.getColumnQualifier().getStatusContexto())) {
                            if (value.trim().equals(config.getStatusInibido())) {
                                inibicao = true;
                            }
                        }
                    }
                    if (timestamp == cell.getTimestamp()) {
                        row.put(column.toUpperCase(),
                                StringUtils.hasText(value) ? value.trim() : "");
                    }
                }
                if (Constants.C_OBITO.equals(contextoOriginal) && !row.isEmpty()) {
                    final Object originalNumCpfCnpj = row.getOrDefault("NUM_CPF_CNPJ", cpf);
                    row.clear();
                    row.put("NUM_CPF_CNPJ", originalNumCpfCnpj);
                }
                if (Constants.C_PPE.equals(contextoOriginal) && !row.isEmpty()) {
                    final Object originalNumCpfCnpj = row.getOrDefault("NUM_CPF_CNPJ", originalCPF);
                    row.remove("NUM_CPF");
                    row.put("NUM_CPF_CNPJ", originalNumCpfCnpj);
                }
                if (!inibicao) {
                    result.add(row);
                } else {
                    isInhibitedForSerpro = true;
                }
            }

            if (result.isEmpty() && !isInhibitedForSerpro) {
                Map<String, Object> serproRow = this.searchInSerpro(cpf, contextoOriginal, requestContext);
                if (serproRow != null) {
                    result.add(serproRow);
                }
            }
        } catch (NotFoundException nf) {
            LOGGER.error("Tabela nao encontrada: {} {}", tableName, nf);
        } catch (IOException io) {
            LOGGER.error("Falha ao consultar tabela: {} {}", tableName, io);
        } catch (NullPointerException e) {
            printLogger(LOGGER, "fim da consulta", contexto, md5Hash(requestMap.get("cpf")), started);
        } catch (ApiTimeoutException t) {
            LOGGER.error("Tempo esgotado para solicitacao {}", t);
        }

        printLogger(LOGGER, "bigTableScan", contexto, md5Hash(requestMap.get("cpf")), started);

        return result;
    }

    public long buscaRecente(Row r) {
        long timestamp = 0;
        for (RowCell cell : r.getCells()) {
            if (cell.getTimestamp() >= timestamp) {
                timestamp = cell.getTimestamp();
            }
        }
        return timestamp;
    }

    private Optional<ContextConverter<?>> getConverter(String context) {
        return this.contextConverters.stream()
                .filter(it -> it.isApplicable(context))
                .findFirst();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> searchInSerpro(String key, String searchContext, String requestContext) {
        try {
            if (key == null || key.trim().isEmpty()) {
                return null;
            }
            Optional<ContextConverter<?>> converter = this.getConverter(searchContext);
            if (!SERPRO_CONTEXTS.contains(searchContext) || !converter.isPresent()) {
                return null;
            }
            if (SERPRO_PHYSICAL_PERSON_CONTEXTS.contains(searchContext)) {
                boolean onlyFromCache = SERPRO_PHYSICAL_PERSON_CONTEXTS_ONLY_CACHE.contains(searchContext) ||
                        requestContext == null ||
                        !SERPRO_PHYSICAL_PERSON_REQUEST_CONTEXTS_ONLY_TRIGGER.contains(requestContext);
                String ni = key.length() == 14 ? key.substring(3) : key;
                return this.serproSearchService.searchPhysicalPerson(ni, onlyFromCache)
                        .map(it -> ((ContextConverter<PhysicalPersonData>) converter.get()).toMap(it))
                        .orElse(null);
            } else {
                return this.serproSearchService.searchLegalPerson(key)
                        .map(it -> ((ContextConverter<LegalPersonData>) converter.get()).toMap(it))
                        .orElse(null);
            }
        } catch (SerproDocumentNotFoundException | SerproUnderageException e) {
            return null;
        } catch (Exception e) {
            LOGGER.error("Falha ao consultar dados da SERPRO", e);
            return null;
        }
    }
}
