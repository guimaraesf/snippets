package br.com.bs.endereco.bigtable;

import br.com.bs.bigtable.BigTableScan;
import br.com.bs.bigtable.config.BigTableConfig;
import br.com.bs.config.AppConfig;
import br.com.bs.interfaces.DatabaseEnderecoCepScan;
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
public class BigTableEnderecoCepScan implements DatabaseEnderecoCepScan {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
    private final BigTableScan bigtablescan;
    private final BigTableConfig bigtableconfig;
    private final AppConfig appConfig;
    public static BigtableDataClient dataClient;

    @Autowired
    public BigTableEnderecoCepScan(BigTableScan bigtablescan, BigTableConfig bigtableconfig, AppConfig appConfig) {
        this.bigtablescan = bigtablescan;
        this.appConfig = appConfig;
        this.bigtableconfig = bigtableconfig;
    }

    @Override
    public Optional<List<Map<String, Object>>> endereco_cep(final String cep) {
        return this.scanPrefixTable(cep);
    }


    public Optional<List<Map<String, Object>>> scanPrefixTable(final String cep) {

        String tableName = "base_cadastral_endereco_new_produto";

        final Instant started = Instant.now();
        printLogger(LOGGER, "ENDERECO ", "Busca por CEP", "Início do processo de consulta de endereco por CEP", started);
        try {
            dataClient = BigtableDataClient.create(bigtableconfig.getProjectId(), bigtableconfig.getInstanceId());
        } catch (IOException e) {
            LOGGER.error("Ocorreu erro ao solicitar conexao com BigTable: {}", e);
        }

        List<Map<String, Object>> resultList = new ArrayList<>();
        long limit = 100L;

        String chave = cep + "-";
        Filters.Filter filter = FILTERS.limit().cellsPerColumn(1);
        Query query = Query.create(tableName).prefix(chave).filter(filter).limit(limit);
        ServerStream<Row> rows = dataClient.readRows(query);

        try {
            if (rows != null) {
                rows.forEach(r -> {
                    Map<String, Object> row = new HashMap<>();
                    r.getCells().forEach(cell -> {
                        switch (cell.getQualifier().toStringUtf8()) {
                            case "endr_tip_lgdo":
                            case "endr_cpto":
                            case "endr_nro":
                            case "endr_cmpl":
                            case "endr_nom_bai":
                            case "endr_cep":
                            case "endr_nom_mncp":
                            case "uf":
                                row.put(cell.getQualifier().toStringUtf8().toUpperCase(),
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim().replace("\"","") : "");
                                break;
                            default:
                                break;
                        }
                    });

                    if (!isBlankRow(row)) {
                        resultList.add(row);
                    }
                });
            }
        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada: {}", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta: {}", e);
        }

        final Instant ended = Instant.now();
        printLogger(LOGGER, "ENDERECO ", "Busca por CEP", "Final do processo de consulta de endereco por CEP", ended);

        dataClient.close();
        return Optional.of(resultList);
    }
}
