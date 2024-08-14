package br.com.bs.ppe.infrastructure.repository.bigtable.impl;

import br.com.bs.bigtable.config.BigTableConfig;
import br.com.bs.config.AppConfig;
import br.com.bs.ppe.infrastructure.repository.bigtable.NomeBigTableRepository;
import br.com.bs.ppe.infrastructure.repository.bigtable.connection.BigTableConnection;
import br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel.NameEntityModel;
import br.com.bs.util.Util;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static br.com.bs.util.Util.printLogger;
import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

@Repository
@Profile("bigtable")
public class NomeBigTableRepositoryImpl implements NomeBigTableRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(NomeBigTableRepositoryImpl.class);

    private final BigTableConnection bigTableConnection;
    private final BigTableConfig bigTableConfig;
    private final AppConfig appConfig;

    public NomeBigTableRepositoryImpl(BigTableConnection bigTableConnection, BigTableConfig bigTableConfig, AppConfig appConfig) {
        this.bigTableConnection = bigTableConnection;
        this.bigTableConfig = bigTableConfig;
        this.appConfig = appConfig;
    }

    @Override
    public Optional<NameEntityModel> findByDocument(String document) {
        try (BigtableDataClient dataClient = this.bigTableConnection.createDataClient()) {
            return this.findByDocument(dataClient, document);
        } catch (IOException e) {
            LOGGER.error("Ocorreu erro ao solicitar conexao com BigTable", e);
            return Optional.empty();
        }
    }

    private Optional<NameEntityModel> findByDocument(BigtableDataClient dataClient, String document) {
        List<Map<String, String>> resultList = new ArrayList<>();
        try {
            Filters.Filter filter = FILTERS.limit()
                    .cellsPerColumn(1);

            String cpf = document.length() == 11 ? "000".concat(document) : document;

            String cpfHash = Util.md5Hash(cpf) + "-0";

            Query query = Query.create(this.bigTableConfig.getTableName(NameEntityModel.CONTEXT, bigTableConfig.getTableProduto()))
                    .rowKey(cpfHash)
                    .filter(filter);

            ServerStream<Row> rows = dataClient.readRows(query);
            resultList = PPEBigTableRepositoryImpl.extractFields(rows, Arrays.asList(
                    NameEntityModel.FIELD_DOCUMENT,
                    NameEntityModel.FIELD_BIRTHDAY,
                    this.appConfig.getColumnQualifier().getStatusContexto()
            ));
        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta", e);
        }

        final Instant ended = Instant.now();
        printLogger(LOGGER, "Nome", "Nome", "Final do processo de consulta de Nome", ended);
        return resultList.stream()
                .findFirst()
                .map(it -> {
                    it.computeIfPresent(
                            this.appConfig.getColumnQualifier().getStatusContexto(),
                            (key, value) -> value.trim().equals(this.appConfig.getStatusInibido()) ? "Y" : "N"
                    );
                    return it;
                })
                .filter(it -> "N".equals(it.getOrDefault(this.appConfig.getColumnQualifier().getStatusContexto(), "N")))
                .map(it -> new NameEntityModel(it));
    }

    @Override
    public List<NameEntityModel> findByDocumentList(List<String> documents) {
        try (BigtableDataClient dataClient = this.bigTableConnection.createDataClient()) {
            return documents.stream()
                    .map(it -> CompletableFuture.supplyAsync(() -> this.findByDocument(dataClient, it)))
                    .map(it -> it.join())
                    .filter(it -> it.isPresent())
                    .map(it -> it.get())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            LOGGER.error("Ocorreu erro ao solicitar conexao com BigTable", e);
            return Collections.emptyList();
        }
    }

}
