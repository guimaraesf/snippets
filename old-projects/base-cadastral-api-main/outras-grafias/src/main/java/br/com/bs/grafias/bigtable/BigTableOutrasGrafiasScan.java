package br.com.bs.grafias.bigtable;

import br.com.bs.bigtable.BigTableScan;
import br.com.bs.bigtable.config.BigTableConfig;
import br.com.bs.config.AppConfig;
import br.com.bs.interfaces.DatabaseOutrasGrafiasScan;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static br.com.bs.util.Util.isBlankRow;
import static br.com.bs.util.Util.printLogger;
import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;


@Profile("bigtable")
@Service
public class BigTableOutrasGrafiasScan implements DatabaseOutrasGrafiasScan {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
    private final BigTableScan bigtablescan;
    private final BigTableConfig bigtableconfig;
    private final AppConfig appConfig;
    public static BigtableDataClient dataClient;

    @Autowired
    public BigTableOutrasGrafiasScan(BigTableScan bigtablescan, BigTableConfig bigtableconfig, AppConfig appConfig) {
        this.bigtablescan = bigtablescan;
        this.appConfig = appConfig;
        this.bigtableconfig = bigtableconfig;
    }

    @Override
    public Optional<List<Map<String, Object>>> outras_grafias(final String cpf) {
        return this.scanTable(cpf);
    }

    public Optional<List<Map<String, Object>>> scanTable(final String cpf) {

        String tableName = "base_cadastral_nome_produto";
        String tableTel = "base_cadastral_telefone_produto";
        String tableEnd = "base_cadastral_endereco_new_produto";

        AtomicReference<String> codDDD = new AtomicReference<>();
        AtomicReference<String> numTel = new AtomicReference<>();
        AtomicReference<String> flagTel = new AtomicReference<>();
        AtomicReference<String> flagEnd = new AtomicReference<>();
        AtomicReference<String> numCep = new AtomicReference<>();
        AtomicReference<String> nomeCond = new AtomicReference<>();

        List<String> listaCep = new ArrayList<>();

        final Instant started = Instant.now();

        Filters.Filter filter = FILTERS.limit().cellsPerColumn(1);
        List<Map<String, Object>> resultList = new ArrayList<>();
        List<Map<String, Object>> resultNomeList = new ArrayList<>();
        long limit = 15L;

        try (BigtableDataClient dataClient = BigtableDataClient.create(bigtableconfig.getProjectId(), bigtableconfig.getInstanceId())) {
            String cpfHash = Util.md5Hash(cpf);
            String cpfNome = cpfHash + "-0";
            Query query = Query.create(tableName).prefix(cpfNome).filter(filter);

            ServerStream<Row> rows = dataClient.readRows(query);

            if (rows != null) {
                LOGGER.info("################ Achou dados na tabela de nome para cpf -> {}", cpf);
                rows.forEach(r -> {
                    Map<String, Object> row = new HashMap<>();

                    r.getCells().forEach(cell -> {
                        switch (cell.getQualifier().toStringUtf8()) {
                            case "num_cpf_cnpj":
                            case "nom_raz_soc":
                            case "dat_nsc":
                                row.put(cell.getQualifier().toStringUtf8().toUpperCase(),
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                            case "dsc_cond":
                                nomeCond.set(StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "Ativo");
                                break;
                        }
                    });
                    if (!isBlankRow(row) && nomeCond.get().equals("Ativo")) {
                        resultNomeList.add(row);
                    }
                });
            }

            /* consulta telefones e armazena quem está no scpc fone*/
            Query queryTel = Query.create(tableTel).prefix(cpfHash).filter(filter).limit(limit);
            ServerStream<Row> rowsTel = dataClient.readRows(queryTel);
            if (rowsTel != null && resultNomeList.size() > 0) {
                LOGGER.info("################ Achou dados na tabela de telefone para cpf -> {}", cpf);
                LOGGER.info("################ Chave utilizada -> {}", cpfHash);
                rowsTel.forEach(r -> {
                    Map<String, Object> row = new HashMap<>();
                    r.getCells().forEach(cell -> {
                        switch (cell.getQualifier().toStringUtf8()) {
                            case "cod_ddd":
                                codDDD.set(StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                row.put(cell.getQualifier().toStringUtf8().toUpperCase(),
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                            case "num_tel":
                                numTel.set(StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                row.put(cell.getQualifier().toStringUtf8().toUpperCase(),
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                            case "dsc_cond":
                                if (cell.getValue().toStringUtf8().trim().equals("Inibido")) {
                                    flagTel.set("1");
                                } else {
                                    flagTel.set("0");
                                }
                                break;
                        }
                    });
                    if (!isBlankRow(row) && flagTel.toString().equals("0")) {
                        String telefone = codDDD.get() + (numTel.get().startsWith("0") ? numTel.get().substring(1, 9) : numTel.get());
                        LOGGER.info("################ Telefone para busca no SCPC FONE -> {}", telefone);
                        Optional<List<Map<String, Object>>> scpcFone = scanScpcFone(dataClient, telefone, cpf);
                        LOGGER.info("################ Retornou busca no SCPC FONE com tamanho de retorno -> {}", scpcFone.get().size());

                        if (scpcFone.get().size() > 0) {
                            //List<Map<String,Object>> teste = scpcFone.get();
                            for (int i = 0; i < scpcFone.get().size(); i++) {
                                if (scpcFone.get().get(i).containsKey("ENDR_CEP")) {
                                    listaCep.add(scpcFone.get().get(i).get("ENDR_CEP").toString());
                                }
                                row.putAll(scpcFone.get().get(i));
                            }
                            for (int i = 0; i < resultNomeList.size(); i++) {
                                row.putAll(resultNomeList.get(i));
                            }
                            resultList.add(row);
                        }
                    }
                });
            }
            /* consulta enderecos e armazena quem não veio pelo scpc fone*/
            Query queryEnd = Query.create(tableEnd).prefix(cpfHash).filter(filter).limit(limit);
            ServerStream<Row> rowsEnd = dataClient.readRows(queryEnd);
            if (rowsEnd != null && resultNomeList.size() > 0) {
                LOGGER.info("################ Achou dados na tabela de endereco para cpf -> {}", cpf);
                rowsEnd.forEach(r -> {
                    Map<String, Object> row = new HashMap<>();
                    r.getCells().forEach(cell -> {
                        switch (cell.getQualifier().toStringUtf8()) {
                            case "endr_cep":
                                numCep.set(StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                row.put(cell.getQualifier().toStringUtf8().toUpperCase(),
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                            case "cod_tip_endr":
                                if (cell.getValue().toStringUtf8().trim().equals("COD_TIP_ENDR_1")) {
                                    row.put("COD_TPO_EDR", "1");
                                } else if (cell.getValue().toStringUtf8().trim().equals("COD_TIP_ENDR_3")) {
                                    row.put("COD_TPO_EDR", "3");
                                } else {
                                    row.put("COD_TPO_EDR", "0");
                                }
                                break;
                            case "endr_cpto":
                            case "endr_nro":
                            case "endr_cmpl":
                            case "endr_nom_bai":
                            case "endr_tip_lgdo":
                            case "endr_nom_mncp":
                            case "uf":
                                row.put(cell.getQualifier().toStringUtf8().toUpperCase(),
                                        StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "");
                                break;
                            case "dsc_cond":
                                if (cell.getValue().toStringUtf8().trim().equals("Inibido")) {
                                    flagEnd.set("1");
                                } else {
                                    flagEnd.set("0");
                                }
                                break;
                        }
                    });
                    if (!isBlankRow(row) && !flagEnd.toString().equals("1")) {
                        LOGGER.info("################ Tamanho da lista de CEP -> {}", listaCep.size());
                        for (int i = 0; i < listaCep.size(); i++) {
                            LOGGER.info("################ Item da lista de CEP -> {}", listaCep.get(i));
                            LOGGER.info("################ CEP obtido em endereco_new -> {}", numCep.toString());
                            if (numCep.toString().equals(listaCep.get(i))) {
                                flagEnd.set("1");
                            }
                        }
                        if (flagEnd.toString().equals("0")) {
                            for (int i = 0; i < resultNomeList.size(); i++) {
                                row.putAll(resultNomeList.get(i));
                            }
                            resultList.add(row);
                        }
                    }
                });
            }
        } catch (IOException e) {
            LOGGER.error("Ocorreu erro ao solicitar conexao com BigTable {}", e);
        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada {}", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta {}", e);
        }

        final Instant ended = Instant.now();
        printLogger(LOGGER, "Outras Grafias ", "Identificacao", "Final do processo de consulta de Outras Grafias", ended);

        return Optional.of(resultList);
    }

    public Optional<List<Map<String, Object>>> scanScpcFone(BigtableDataClient dataClient, final String telefone, String cpf) {
        String tableName = "base_cadastral_scpc_fone_produto";

        final Instant started = Instant.now();
        printLogger(LOGGER, "SCPC Fone", "scpc fone", telefone, started);

        long limit = 1L;
        ArrayList<String> campos = new ArrayList<String>();
        ArrayList<String> campos_valida_cond = new ArrayList<String>();
        ArrayList<String> campos_valida_sig = new ArrayList<String>();
        campos.add("CTELKNUM");
        campos.add("CTELKDDD");
        campos.add("CTELLOGR");
        campos.add("CTELENDR");
        campos.add("CTELNUMR");
        campos.add("CTELCOMR");
        campos.add("CTELBAIC");
        campos.add("CTELNCEP");
        campos.add("CTELCIDC");
        campos.add("CTELUFED");
        campos.add("CTELNCPF");

        campos_valida_cond.add("CTELCOND");
        campos_valida_sig.add("CTELTSIG");
        String num_tel_pre = "0";
        String ddd = "00" + telefone.trim().substring(0, 2);
        if (telefone.trim().substring(2).equals(9)) {
            num_tel_pre = "";
        }

        String num_tel = num_tel_pre + telefone.trim().substring(2);
        String telefone_tratado = ddd + num_tel;

        Filters.Filter filter = FILTERS.limit().cellsPerColumn(1);
        List<Map<String, Object>> resultList = new ArrayList<>();

        try {
            String telefoneHash = Util.md5Hash(telefone_tratado);
            Query query = Query.create(tableName).prefix(telefoneHash).filter(filter).limit(limit);
            AtomicReference<String> cpfCtel = new AtomicReference<>("000");

            ServerStream<Row> rows = dataClient.readRows(query);
            AtomicBoolean inibicao_a = new AtomicBoolean(false);
            AtomicBoolean inibicao_s = new AtomicBoolean(false);

            if (rows != null) {
                LOGGER.info("################ Achou dados na tabela de SCPC FONE para telefone -> {}", telefoneHash);
                rows.forEach(r -> {
                    final Map<String, Object> row = new HashMap<>();
                    r.getCells().forEach(cell -> {
                        String coluna_verifica = cell.getQualifier().toStringUtf8().toUpperCase().trim();
                        if (campos.indexOf(coluna_verifica) != -1) {
                            switch (coluna_verifica) {
                                case "CTELNCPF":
                                    cpfCtel.set(cell.getValue().toStringUtf8());
                                    break;
                                case "CTELUFED":
                                    row.put("UF", cell.getValue().toStringUtf8());
                                    break;
                                case "CTELENDR":
                                    row.put("COD_TPO_EDR", "1");
                                    row.put("ENDR_CPTO", cell.getValue().toStringUtf8());
                                    break;
                                case "CTELNUMR":
                                    row.put("ENDR_NRO", cell.getValue().toStringUtf8());
                                    break;
                                case "CTELLOGR":
                                    row.put("ENDR_TIP_LGDO", cell.getValue().toStringUtf8());
                                    break;
                                case "CTELCOMR":
                                    row.put("ENDR_CMPL", cell.getValue().toStringUtf8());
                                    break;
                                case "CTELNCEP":
                                    row.put("ENDR_CEP", cell.getValue().toStringUtf8());
                                    break;
                                case "CTELBAIC":
                                    row.put("ENDR_NOM_BAI", cell.getValue().toStringUtf8());
                                    break;
                                case "CTELCIDC":
                                    row.put("ENDR_NOM_MNCP", cell.getValue().toStringUtf8());
                                    break;
                                default:
                                    break;
                            }
                        }
                        if (campos_valida_cond.indexOf(coluna_verifica) != -1) {
                            if (!cell.getValue().toStringUtf8().trim().equals("A")) {
                                inibicao_a.set(true);
                            }
                        }
                        if (campos_valida_sig.indexOf(coluna_verifica) != -1) {
                            if (cell.getValue().toStringUtf8().trim().equals("1")) {
                                inibicao_s.set(true);
                            }
                        }
                    });
                    if (inibicao_a.get() == false) {
                        String cpfFormatado = "000".concat(cpfCtel.get());
                        LOGGER.info("################ CPF para comparacao -> {}", cpf);
                        LOGGER.info("################ CPF obtido na CTEL -> {}", cpfFormatado);
                        if (cpfFormatado.equals(cpf) && !inibicao_s.get()) {
                            resultList.add(row);
                        }
                    }
                });
            }
        } catch (NotFoundException e) {
            LOGGER.error("Tabela nao encontrada {}", e);
        } catch (NullPointerException e) {
            LOGGER.error("Fim da consulta {}", e);
            return Optional.of(Collections.emptyList());
        }

        final Instant ended = Instant.now();
        printLogger(LOGGER, "SCPC Fone", "scpc fone", telefone, ended);

        return Optional.of(resultList);
    }
}
