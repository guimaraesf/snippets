package br.com.bs.ppe.infrastructure.repository.bigtable.impl;

import br.com.bs.ppe.infrastructure.repository.bigtable.PPEBigTableRepository;
import br.com.bs.ppe.infrastructure.repository.bigtable.entitymodel.*;
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
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static br.com.bs.util.Util.isBlankRow;
import static br.com.bs.util.Util.printLogger;
import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

@Repository
@Profile("bigtable")
public class PPEBigTableRepositoryImpl implements PPEBigTableRepository {

    private final Logger LOGGER = LoggerFactory.getLogger(PPEBigTableRepositoryImpl.class);
    private static final long LIMIT_RELATIONSHIPS = 50;
    private static final long LIMIT_MANDATES = 50;

    @Override
    public Optional<PPEPessoasEntityModel> findPPEPessoas(BigtableDataClient dataClient, String document) {
        List<Map<String, String>> resultList = new ArrayList<>();
        final Instant started = Instant.now();

        try {
            Filters.Filter filter = FILTERS.limit()
                    .cellsPerColumn(1);

            String cpfHash = Util.md5Hash(document) + "-0";

            Query query = Query.create(PPEPessoasEntityModel.TABLE)
                    .rowKey(cpfHash)
                    .filter(filter);

            ServerStream<Row> rows = dataClient.readRows(query);
            resultList = extractFields(rows, Arrays.asList(
                    PPEPessoasEntityModel.FIELD_CPF,
                    PPEPessoasEntityModel.FIELD_NAME,
                    PPEPessoasEntityModel.FIELD_BIRTHDAY,
                    PPEPessoasEntityModel.FIELD_TYPE_CODE
            ));
        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta", e);
        }

        printLogger(LOGGER, "PPE", "PPE Pessoa", "Final do processo de consulta de PPE", started);
        return resultList.stream()
                .findFirst()
                .map(it -> new PPEPessoasEntityModel(it));
    }

    @Override
    public Optional<OrganizationEntityModel> findOrganization(BigtableDataClient dataClient, String code) {
        List<Map<String, String>> resultList = new ArrayList<>();
        final Instant started = Instant.now();
        try {
            Filters.Filter filter = FILTERS.limit()
                    .cellsPerColumn(1);

            Query query = Query.create(OrganizationEntityModel.TABLE)
                    .rowKey(code)
                    .filter(filter);

            ServerStream<Row> rows = dataClient.readRows(query);
            resultList = extractFields(rows, Arrays.asList(
                    OrganizationEntityModel.FIELD_ORGANIZATION_CODE,
                    OrganizationEntityModel.FIELD_ORGANIZATION_DESCRIPTION,
                    OrganizationEntityModel.FIELD_HIGHER_ORGANIZATION_DESCRIPTION,
                    OrganizationEntityModel.FIELD_WEBSITE,
                    OrganizationEntityModel.FIELD_ESFERA_PODER_CODE,
                    OrganizationEntityModel.FIELD_ESFERA_PODER_DESCRIPTION
            ));
        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta", e);
        }

        printLogger(LOGGER, "PPE", "PPE Orgao", "Final do processo de consulta de PPE", started);
        return resultList.stream()
                .findFirst()
                .map(it -> new OrganizationEntityModel(it));
    }

    @Override
    public Optional<OrganizationAddressEntityModel> findOrganizationAddress(BigtableDataClient dataClient, String organizationCode, String mandateCode) {
        return this.findOrganizationAddress(dataClient,
                mandateCode +
                        "#" +
                        organizationCode
        );

    }

    @Override
    public Optional<OrganizationAddressEntityModel> findOrganizationAddress(BigtableDataClient dataClient, String rowKey) {
        List<Map<String, String>> resultList = new ArrayList<>();
        final Instant started = Instant.now();
        try {
            Filters.Filter filter = FILTERS.limit()
                    .cellsPerColumn(1);

            Query query = Query.create(OrganizationAddressEntityModel.TABLE)
                    .rowKey(rowKey)
                    .filter(filter);

            ServerStream<Row> rows = dataClient.readRows(query);
            resultList = extractFields(rows, Arrays.asList(
                    OrganizationAddressEntityModel.FIELD_CODE,
                    OrganizationAddressEntityModel.FIELD_ADDRESS_TYPE,
                    OrganizationAddressEntityModel.FIELD_STREET,
                    OrganizationAddressEntityModel.FIELD_STREET_NUMBER,
                    OrganizationAddressEntityModel.FIELD_STREET_COMPLEMENT,
                    OrganizationAddressEntityModel.FIELD_CITY,
                    OrganizationAddressEntityModel.FIELD_UF,
                    OrganizationAddressEntityModel.FIELD_ZIP_CODE
            ));
        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta", e);
        }

        printLogger(LOGGER, "PPE", "PPE Endereco Orgao", "Final do processo de consulta de PPE", started);
        return resultList.stream()
                .findFirst()
                .map(it -> {
                    String[] key = rowKey.split("#");
                    return new OrganizationAddressEntityModel(key[1], key[0], it);
                });
    }

    @Override
    public List<MandateEntityModel> findMandates(BigtableDataClient dataClient, String document) {
        List<Map<String, String>> resultList = new ArrayList<>();
        final Instant started = Instant.now();
        try {
            Filters.Filter filter = FILTERS.limit()
                    .cellsPerColumn(1);

            String cpfHash = Util.md5Hash(document) + "-";

            Query query = Query.create(MandateEntityModel.TABLE)
                    .prefix(cpfHash)
                    .filter(filter)
                    .limit(LIMIT_MANDATES);

            ServerStream<Row> rows = dataClient.readRows(query);
            resultList = extractFields(rows, Arrays.asList(
                    MandateEntityModel.FIELD_CPF,
                    MandateEntityModel.FIELD_INITIAL_DATE,
                    MandateEntityModel.FIELD_END_DATE,
                    MandateEntityModel.FIELD_ESFERA_PODER,
                    MandateEntityModel.FIELD_ORGANIZATION_CODE,
                    MandateEntityModel.FIELD_ORGANIZATION_NAME,
                    MandateEntityModel.FIELD_PUBLIC_OFFICE,
                    MandateEntityModel.FIELD_PUBLIC_OFFICE_CODE
            ));
        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta", e);
        }

        printLogger(LOGGER, "PPE", "PPE Pessoa Mandatos", "Final do processo de consulta de PPE", started);

        return resultList.stream()
                .map(it -> new MandateEntityModel(it))
                .collect(Collectors.toList());

    }

    @Override
    public List<RelationshipEntityModel> findRelationship(BigtableDataClient dataClient, String document, boolean fromTitleholder) {
        List<Map<String, String>> resultList = new ArrayList<>();
        final Instant started = Instant.now();
        try {
            Filters.Filter filter = FILTERS.chain()
                    .filter(FILTERS.family().exactMatch(fromTitleholder ? RelationshipEntityModel.FAMILY_TITLEHOLDER : RelationshipEntityModel.FAMILY_RELATIONSHIP))
                    .filter(FILTERS.limit().cellsPerColumn(1));

            String cpfHash = Util.md5Hash(document);

            Query query = Query.create(RelationshipEntityModel.TABLE)
                    .prefix(cpfHash)
                    .filter(filter)
                    .limit(LIMIT_RELATIONSHIPS);

            ServerStream<Row> rows = dataClient.readRows(query);
            resultList = extractFields(rows, Arrays.asList(
                    RelationshipEntityModel.FIELD_CPF_RELATIONSHIP,
                    RelationshipEntityModel.FIELD_CPF_TITLEHOLDER,
                    RelationshipEntityModel.FIELD_RELATIONSHIP_TYPE
            ));
        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta", e);
        }

        printLogger(LOGGER, "PPE", "PPE Pessoa Relacionados", "Final do processo de consulta de PPE", started);

        return resultList.stream()
                .map(it -> new RelationshipEntityModel(it))
                .collect(Collectors.toList());
    }

    public static List<Map<String, String>> extractFields(ServerStream<Row> rowServerStream, List<String> fields) {
        List<Map<String, String>> resultList = new ArrayList<>();
        if (rowServerStream != null) {
            rowServerStream.forEach(r -> {
                Map<String, String> row = new HashMap<>();
                r.getCells().stream()
                        .filter(it -> fields.contains(it.getQualifier().toStringUtf8().toUpperCase()))
                        .forEach(it ->
                                row.put(it.getQualifier().toStringUtf8().toUpperCase(),
                                        StringUtils.hasText(it.getValue().toStringUtf8()) ? it.getValue().toStringUtf8().trim() : "")
                        );

                if (!isBlankRow(row)) {
                    resultList.add(row);
                }
            });

        }
        return resultList;
    }

}
