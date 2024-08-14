package br.com.bs.ppe.bigtable;

import br.com.bs.bigtable.BigTableScan;
import br.com.bs.bigtable.config.BigTableConfig;
import br.com.bs.config.AppConfig;
import br.com.bs.interfaces.DatabasePpePessoaScan;
import br.com.bs.util.Util;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

import static br.com.bs.util.Util.isBlankRow;
import static br.com.bs.util.Util.printLogger;
import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;


@Profile("bigtable")
@Service
public class BigTablePpePessoaScan implements DatabasePpePessoaScan {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
    private final BigTableScan bigtablescan;
    private final BigTableConfig bigtableconfig;
    private final AppConfig appConfig;
    public static BigtableDataClient dataClient;

    @Autowired
    public BigTablePpePessoaScan(BigTableScan bigtablescan, BigTableConfig bigtableconfig, AppConfig appConfig) {
        this.bigtablescan = bigtablescan;
        this.appConfig = appConfig;
        this.bigtableconfig = bigtableconfig;
    }

    @Override
    public Optional<List<Map<String, Object>>> ppe_pessoa(final String cpf) {

        return this.scanTable(cpf);
    }

    @Override
    public Optional<List<Map<String, Object>>> ppe_pessoa_nome(final String nome) {
        return this.scanPrefixTable(nome);
    }

    @Override
    public Optional<List<Map<String, Object>>> ppe_pessoa_mandato(final String cpf) {
        return this.scanMandato(cpf);
    }

    @Override
    public Optional<List<Map<String, Object>>> ppe_pessoa_relacionados(final String cpf) {
        return this.scanRelacionados(cpf);
    }

    @Override
    public Optional<List<Map<String, Object>>> ppe_pessoa_titular(final String cpf) {
        return this.scanTitular(cpf);
    }

    @Override
    public Optional<List<Map<String, Object>>> ppe_pessoa_titular_relacionado(final String cpfTitular, final String cpfRelacionado) {
        return this.scanTitularMaisRelacionado(cpfTitular, cpfRelacionado);
    }


    public Optional<List<Map<String, Object>>> scanTable(final String cpf) {

        String tableName = "base_cadastral_ppe_pessoas_produto";

        final Instant started = Instant.now();
        try {
            dataClient = BigtableDataClient.create(bigtableconfig.getProjectId(), bigtableconfig.getInstanceId());
        } catch (IOException e) {
            LOGGER.error("Ocorreu erro ao solicitar conexao com BigTable {}", e);
        }

        Filters.Filter filter = FILTERS.limit().cellsPerColumn(1);
        List<Map<String, Object>> resultList = new ArrayList<>();
        int limit = 100;

        try {
            String cpfHash = Util.md5Hash(cpf) + "-0";
            Query query = Query.create(tableName).prefix(cpfHash).filter(filter);

            ServerStream<Row> rows = dataClient.readRows(query);

            if (rows != null) {
                rows.forEach(r -> {
                    Map<String, Object> row = new HashMap<>();
                    r.getCells().forEach(cell -> {
                        switch (cell.getQualifier().toStringUtf8()) {
                            case "NUM_CPF":
                            case "NOM_PPE":
                            case "DAT_NSC":
                            case "COD_TPO_PPE":
                                row.put(cell.getQualifier().toStringUtf8().toUpperCase(),
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                        }
                    });
                    if (!isBlankRow(row)) {
                        resultList.add(row);
                    }
                });

            }
        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada {}", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta {}", e);
        }

        final Instant ended = Instant.now();
        printLogger(LOGGER, "PPE ", "PPE Pessoa", "Final do processo de consulta de PPE", ended);

        dataClient.close();
        return Optional.of(resultList);

    }

    public Optional<List<Map<String, Object>>> scanPrefixTable(final String nome) {

        String tableName = "base_cadastral_ppe_pessoas_produto";

        final Instant started = Instant.now();
        try {
            dataClient = BigtableDataClient.create(bigtableconfig.getProjectId(), bigtableconfig.getInstanceId());
        } catch (IOException e) {
            LOGGER.error("Ocorreu erro ao solicitar conexao com BigTable {}", e);
        }

        List<Map<String, Object>> resultList = new ArrayList<>();
        long limit = 100L;

        Filters.Filter filter = FILTERS.limit().cellsPerColumn(1);
        Query query = Query.create(tableName).prefix(nome).filter(filter).limit(limit);
        ServerStream<Row> rows = dataClient.readRows(query);

        try {
            if (rows != null) {
                rows.forEach(r -> {
                    Map<String, Object> row = new HashMap<>();
                    r.getCells().forEach(cell -> {
                        switch (cell.getQualifier().toStringUtf8()) {
                            case "NUM_CPF":
                            case "NOM_PPE":
                            case "DAT_NSC":
                            case "COD_TPO_PPE":
                                row.put(cell.getQualifier().toStringUtf8().toUpperCase(),
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                        }
                    });

                    if (!isBlankRow(row)) {
                        resultList.add(row);
                    }
                });
            }
        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada {}", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta {}", e);
        }

        final Instant ended = Instant.now();
        printLogger(LOGGER, "PPE ", "PPE Pessoa - Busca por nome", "Final do processo de consulta de PPE", ended);

        dataClient.close();
        return Optional.of(resultList);

    }

    public Optional<List<Map<String, Object>>> scanMandato(final String cpf) {

        String tableName = "base_cadastral_ppe_mandatos_produto";

        final Instant started = Instant.now();
        try {
            dataClient = BigtableDataClient.create(bigtableconfig.getProjectId(), bigtableconfig.getInstanceId());
        } catch (IOException e) {
            LOGGER.error("Ocorreu erro ao solicitar conexao com BigTable {}", e);
        }

        Filters.Filter filter = FILTERS.limit().cellsPerColumn(1);
        List<Map<String, Object>> resultList = new ArrayList<>();

        try {
            String cpfHash = Util.md5Hash(cpf) + "-";
            Query query = Query.create(tableName).prefix(cpfHash).filter(filter);
            ServerStream<Row> rows = dataClient.readRows(query);
            if (rows != null) {
                rows.forEach(r -> {
                    Map<String, Object> row = new HashMap<>();
                    r.getCells().forEach(cell -> {
                        switch (cell.getQualifier().toStringUtf8()) {
                            case "NUM_CPF":
                            case "DAT_FIM_ATV":
                            case "DAT_INI_ATV":
                            case "DSC_PPE_ESF_PDR":
                            case "DSC_PPE_ORG":
                            case "DSC_PPE_CRG":
                                row.put(cell.getQualifier().toStringUtf8().toUpperCase(),
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                        }
                    });
                    if (!isBlankRow(row)) {
                        resultList.add(row);
                    }
                });
            }
        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada {}", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta {}", e);
        }

        final Instant ended = Instant.now();
        printLogger(LOGGER, "PPE ", "PPE Pessoa Mandatos", "Final do processo de consulta de PPE", ended);

        dataClient.close();
        return Optional.of(resultList);

    }

    public Optional<List<Map<String, Object>>> scanRelacionados(final String cpfRel) {

        String tableName = "base_cadastral_ppe_relacionados_produto";

        final Instant started = Instant.now();

        try {
            dataClient = BigtableDataClient.create(bigtableconfig.getProjectId(), bigtableconfig.getInstanceId());
        } catch (IOException e) {
            LOGGER.error("Ocorreu erro ao solicitar conexao com BigTable {}", e);
        }

        List<Map<String, Object>> resultList = new ArrayList<>();
        long limit = 100L;

        Filters.Filter filter = FILTERS.chain()
                .filter(FILTERS.family().exactMatch("titular"))
                .filter(FILTERS.limit().cellsPerColumn(1));
        String cpfHash = Util.md5Hash(cpfRel);
        Query query = Query.create(tableName).prefix(cpfHash).filter(filter).limit(limit);
        ServerStream<Row> rows = dataClient.readRows(query);

        try {
            if (rows != null) {
                rows.forEach(r -> {
                    Map<String, Object> row = new HashMap<>();
                    r.getCells().forEach(cell -> {
                        switch (cell.getQualifier().toStringUtf8()) {
                            case "NUM_CPF_REL":
                                Optional<List<Map<String, Object>>> ppe_pessoa = scanTable(cell.getValue().toStringUtf8().trim());
                                ppe_pessoa.ifPresent(maps -> maps.forEach(map -> map.forEach(row::put)));
                                break;
                            case "NUM_CPF_TIT":
                                row.put("NUM_CPF_REL",
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                            case "NOM_RZO_SCL":
                            case "DSC_TPO_REL":
                                row.put(cell.getQualifier().toStringUtf8().toUpperCase(),
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                        }
                    });

                    if (!isBlankRow(row)) {
                        resultList.add(row);
                    }
                });

            }
        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada {}  ", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta {}", e);
        }

        final Instant ended = Instant.now();
        printLogger(LOGGER, "PPE ", "PPE Pessoa Relacionados", "Final do processo de consulta de PPE", ended);

        dataClient.close();
        return Optional.of(resultList);

    }

    public Optional<List<Map<String, Object>>> scanTitular(final String cpf) {

        String tableName = "base_cadastral_ppe_relacionados_produto";

        final Instant started = Instant.now();

        try {
            dataClient = BigtableDataClient.create(bigtableconfig.getProjectId(), bigtableconfig.getInstanceId());
        } catch (IOException e) {
            LOGGER.error("Ocorreu erro ao solicitar conexao com BigTable {}", e);
        }

        List<Map<String, Object>> resultList = new ArrayList<>();
        long limit = 100L;

        Filters.Filter filter = FILTERS.chain()
                .filter(FILTERS.family().exactMatch("relacionados"))
                .filter(FILTERS.limit().cellsPerColumn(1));
        String cpfHash = Util.md5Hash(cpf);
        Query query = Query.create(tableName).prefix(cpfHash).filter(filter).limit(limit);
        ServerStream<Row> rows = dataClient.readRows(query);

        Optional<List<Map<String, Object>>> ppe_pessoa = scanTable(cpf.trim());

        if (!ppe_pessoa.isPresent()) {
            Instant noData = Instant.now();
            printLogger(LOGGER, "PPE ", "PPE Pessoa Relacionados por Titular", "Sem dados de Titular PPE encontrados", noData);
            return Optional.of(resultList);
        }

        try {
            if (rows != null) {
                rows.forEach(r -> {
                    Map<String, Object> row = new HashMap<>();
                    r.getCells().forEach(cell -> {
                        switch (cell.getQualifier().toStringUtf8()) {
                            case "NUM_CPF_REL":
                                ppe_pessoa.ifPresent(maps -> maps.forEach(map -> map.forEach(row::put)));
                                break;
                            case "NUM_CPF_TIT":
//                                ppe_pessoa.ifPresent(maps -> maps.forEach(map -> map.forEach(row::put)));
//                                break;
                            case "DSC_TPO_REL":
                            case "NOM_RZO_SCL":
                                LOGGER.info("Coluna: {} | Valor: {}", cell.getQualifier().toStringUtf8().toUpperCase(),cell.getValue().toStringUtf8().trim());
                                row.put(cell.getQualifier().toStringUtf8().toUpperCase(),
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                        }
                    });

                    if (!isBlankRow(row)) {
                        resultList.add(row);
                    }
                });
            }
        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada {}", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta {}", e);
        }

        final Instant ended = Instant.now();
        printLogger(LOGGER, "PPE ", "PPE Pessoa Relacionados por Titular", "Final do processo de consulta de PPE", ended);

        dataClient.close();
        return Optional.of(resultList);

    }

    public Optional<List<Map<String, Object>>> scanTitularMaisRelacionado(final String cpfTitular, final String cpfRelacionado) {

        String tableName = "base_cadastral_ppe_relacionados_produto";

        final Instant started = Instant.now();
        try {
            dataClient = BigtableDataClient.create(bigtableconfig.getProjectId(), bigtableconfig.getInstanceId());
        } catch (IOException e) {
            LOGGER.error("Ocorreu erro ao solicitar conexao com BigTable {}", e);
        }


        List<Map<String, Object>> resultList = new ArrayList<>();
        long limit = 100L;

        Filters.Filter filter = FILTERS.chain()
                .filter(FILTERS.family().exactMatch("titrel"))
                .filter(FILTERS.limit().cellsPerColumn(1));
        String cpfHash = Util.md5Hash(cpfTitular) + "#" + Util.md5Hash(cpfRelacionado);
        Query query = Query.create(tableName).prefix(cpfHash).filter(filter).limit(limit);
        ServerStream<Row> rows = dataClient.readRows(query);

        try {
            if (rows != null) {
                rows.forEach(r -> {
                    Map<String, Object> row = new HashMap<>();
                    r.getCells().forEach(cell -> {
                        switch (cell.getQualifier().toStringUtf8()) {
                            case "NUM_CPF_TIT":
                                Optional<List<Map<String, Object>>> ppe_pessoa_titular = scanTable(cell.getValue().toStringUtf8().trim());
                                ppe_pessoa_titular.ifPresent(maps -> maps.forEach(map -> map.forEach(row::put)));
                                break;
                            case "NUM_CPF_REL":
                                Optional<List<Map<String, Object>>> ppe_pessoa_relacionada = scanTablePpeRelacionado(cell.getValue().toStringUtf8().trim());
                                ppe_pessoa_relacionada.ifPresent(maps -> maps.forEach(map -> map.forEach(row::put)));
                                break;
                            case "DSC_TPO_REL":
                            case "NOM_RZO_SCL":
                                row.put(cell.getQualifier().toStringUtf8().toUpperCase(),
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                        }
                    });

                    if (!isBlankRow(row)) {
                        resultList.add(row);
                    }
                });
            }

        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada {}", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta {}", e);
        }

        final Instant ended = Instant.now();
        printLogger(LOGGER, "PPE ", "PPE Pessoa Relacionados + Titular", "Final do processo de consulta de PPE", ended);

        return Optional.of(resultList);

    }

    public Optional<List<Map<String, Object>>> scanTablePpeRelacionado(final String cpf) {

        String tableName = "base_cadastral_ppe_pessoas_produto";

        final Instant started = Instant.now();
        try {
            dataClient = BigtableDataClient.create(bigtableconfig.getProjectId(), bigtableconfig.getInstanceId());
        } catch (IOException e) {
            LOGGER.error("Ocorreu erro ao solicitar conexao com BigTable {}", e);
        }

        Filters.Filter filter = FILTERS.limit().cellsPerColumn(1);
        List<Map<String, Object>> resultList = new ArrayList<>();
        int limit = 100;

        try {
            String cpfHash = Util.md5Hash(cpf) + "-0";
            Query query = Query.create(tableName).prefix(cpfHash).filter(filter);

            ServerStream<Row> rows = dataClient.readRows(query);

            if (rows != null) {
                rows.forEach(r -> {
                    Map<String, Object> row = new HashMap<>();
                    r.getCells().forEach(cell -> {
                        switch (cell.getQualifier().toStringUtf8()) {
                            case "NUM_CPF":
                                row.put("NUM_CPF_REL",
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                            case "NOM_PPE":
                                row.put("NOM_PPE_REL",
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                            case "DAT_NSC":
                                row.put("DAT_NSC_REL",
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                            case "COD_TPO_PPE":
                                row.put("COD_TPO_PPE_REL",
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                        }
                    });
                    if (!isBlankRow(row)) {
                        resultList.add(row);
                    }
                });
            }
        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada {}", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta {}", e);
        }

        final Instant ended = Instant.now();
        printLogger(LOGGER, "PPE ", "PPE Relacionado", "Final do processo de consulta de relacionado que é PPE", ended);

        dataClient.close();
        return Optional.of(resultList);

    }

}
