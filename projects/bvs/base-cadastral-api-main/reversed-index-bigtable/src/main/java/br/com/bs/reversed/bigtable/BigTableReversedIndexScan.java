package br.com.bs.reversed.bigtable;

import br.com.bs.bigtable.config.BigTableConfig;
import br.com.bs.config.AppConfig;
import br.com.bs.interfaces.DatabaseReversedIndexScan;
import br.com.bs.bigtable.BigTableScan;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static br.com.bs.util.Util.md5Hash;
import static br.com.bs.util.Util.printLogger;


@Profile("bigtable")
@Service
public class BigTableReversedIndexScan implements DatabaseReversedIndexScan {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
    private final BigTableScan bigtablescan;
    private final BigTableConfig bigtableconfig;
    private final AppConfig appConfig;
    //public static BigtableDataClient dataClient;

    @Autowired
    public BigTableReversedIndexScan(BigTableScan bigtablescan, BigTableConfig bigtableconfig, AppConfig appConfig) {
        this.bigtablescan = bigtablescan;
        this.appConfig = appConfig;
        this.bigtableconfig = bigtableconfig;
    }

    @Override
    public Optional<List<String>> findReverdIndexTable(final String origin, final String verbo) {
        return this.findReverdIndexTable(origin, verbo, null);
    }

    @Override
    public Optional<List<String>> findReverdIndexTable(final String origin, final String verbo, final String destino) {
        return scanTable(origin, verbo, destino);
    }

    private Optional<List<String>> scanTable(final String origin, final String verbo, final String destino) {
        /*
         * rowkey = origin = obrigatorio
         * familia = verbo = obrigatorio
         * coluna = destino = opcional
         * resultado com order by no valor da celula
         * contexto nome-nascimento = nome-nascimento=familia:DTNasc=columnQualifier (pode filtrar por destino - data) e retornou o CPF
         * */

        String tableName = "base_cadastral_" + appConfig.getReversedIndex().getTableName() + "_produto";

        final Instant started = Instant.now();

//        try {
//            dataClient = BigtableDataClient.create(bigtableconfig.getProjectId(), bigtableconfig.getInstanceId());
//        } catch (IOException e) {
//            LOGGER.info("Ocorreu erro ao solicitar conexao com BigTable {}", e);
//        }

        final Set<String> resultList = new HashSet<>();
        int qtdResultados=0;
        int limit = bigtableconfig.getLimitRows();

//        Filters.Filter filter = FILTERS.chain()
//                .filter(FILTERS.family().exactMatch(verbo))
//                .filter(FILTERS.limit().cellsPerColumn(1));
//        Query query = Query.create(tableName).rowKey(origin.toUpperCase()).filter(filter).limit(limit);

        try {

            Row r = this.bigtableconfig.buildConnection().readRow(tableName, origin.toUpperCase());

            for (RowCell cell : r.getCells()) {
                if ((cell.getFamily().toLowerCase().equalsIgnoreCase(verbo))){
                    if(destino != null) {
                        if (destino.trim().length() > 0) {
                            if (cell.getQualifier().toStringUtf8().equalsIgnoreCase(destino)) {
                                resultList.add(cell.getValue().toStringUtf8());
                                qtdResultados++;
                            }
                        }
                    }
                    else{
                        resultList.add(cell.getQualifier().toStringUtf8());
                        qtdResultados++;
                    }
                }
                if(qtdResultados==limit){
                    break;
                }
            }
        } catch (NotFoundException e){
            LOGGER.error("Tabela nao encontrada {}",e);
        } catch(IOException io){
            LOGGER.error("Falha ao consultar tabela: {}",io.getMessage());
        } catch(NullPointerException e){
            //LOGGER.error("Nao foi possivel iterar a linha: ",e.getMessage());
            printLogger(LOGGER, "fim da consulta", "reversedIndex", md5Hash(verbo), started);
        }

        /* evolução de consultas
//        try {
//            ServerStream<Row> rows = dataClient.readRows(query);
//            rows.forEach(r -> r.getCells().forEach(cell -> {
//                Map<String, Long> rowCompare = new HashMap<>();
//                if (destino != null) {
//                    if (destino.trim().length() > 0) {
//                        if (cell.getQualifier().toStringUtf8().equalsIgnoreCase(destino)) {
//                            rowCompare.put(StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "",
//                                    cell.getTimestamp());
//                            resultListCompare.add(rowCompare);
//                        }
//                    }
//                } else {
//                    rowCompare.put(StringUtils.hasText(cell.getValue().toStringUtf8()) ? cell.getValue().toStringUtf8().trim() : "",
//                            cell.getTimestamp());
//                    resultListCompare.add(rowCompare);
//                }
//            }));
//
//            AtomicLong max = new AtomicLong();
//            resultListCompare.forEach(r -> {
//                max.set(r.values().stream().max(Comparator.naturalOrder()).get());
//            });
//            resultListCompare.forEach(r -> {
//                resultList[0] = r.entrySet()
//                        .stream()
//                        .filter(e -> e.getValue() == max.get())
//                        .map(Map.Entry::getKey)
//                        .collect(Collectors.toSet());
//            });
//        } catch (NotFoundException e) {
//            LOGGER.info("Tabela nao encontrada " + e);
//        } catch (NullPointerException e) {
//            LOGGER.info("Nao foi possivel iterar a linha: " + e);
//        }
*/

        printLogger(LOGGER, "bigTableReversedIndexScan", origin, verbo, started);

        return Optional.of(orderResults(resultList, origin, verbo));
    }


    private List<String> orderResults(final Set<String> resultList, final String origin, final String verbo) {
        if (resultList == null || resultList.isEmpty())
            return Collections.emptyList();

        final Instant startedOrder = Instant.now();
        List<String> sorted = resultList.stream().sorted().collect(Collectors.toList());
        printLogger(LOGGER, "bigTableReversedperformOrder", origin, verbo, startedOrder);

        return sorted;
    }
}
