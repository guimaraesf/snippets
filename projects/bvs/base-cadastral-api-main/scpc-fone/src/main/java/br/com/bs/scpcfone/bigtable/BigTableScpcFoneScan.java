package br.com.bs.scpcfone.bigtable;

import java.io.IOException;
import br.com.bs.bigtable.config.BigTableConfig;
import br.com.bs.config.AppConfig;
import br.com.bs.interfaces.DatabaseScpcFoneScan;
import br.com.bs.util.Util;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import br.com.bs.bigtable.BigTableScan;
import java.util.ArrayList;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;


import java.time.Instant;
import java.util.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import static br.com.bs.util.Util.md5Hash;
import static br.com.bs.util.Util.printLogger;
import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;


@Profile("bigtable")
@Service
public class BigTableScpcFoneScan implements DatabaseScpcFoneScan {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
    private final BigTableScan bigtablescan;
    private final BigTableConfig bigtableconfig;
	public static BigtableDataClient dataClient;
    private final AppConfig appConfig;

    @Autowired
    public BigTableScpcFoneScan (BigTableScan bigtablescan, BigTableConfig bigtableconfig, AppConfig appConfig){
        this.bigtablescan = bigtablescan;
        this.appConfig = appConfig;
        this.bigtableconfig = bigtableconfig;
    }

    @Override
    public Optional<List<Map<String, Object>>> findScpcFonTable(final String telefone) {
            return this.scanTableOriginal(telefone);
    }

//    public Optional<List<Map<String, Object>>> scanTable(final String telefone) {
//
//		String tableName = "base_cadastral_scpc_fone_produto";
//
//        final Instant started = Instant.now();
//		printLogger(LOGGER, "SCPC Fone", "scpc fone", telefone, started);
//
//        long limit=1L;
//		ArrayList<String> campos = new ArrayList<String>();
//		ArrayList<String> campos_valida_cond = new ArrayList<String>();
//		ArrayList<String> campos_valida_sig = new ArrayList<String>();
//		campos.add("CTELKNUM");
//		campos.add("CTELKDDD");
//		campos.add("CTELLOGR");
//		campos.add("CTELENDR");
//		campos.add("CTELNUMR");
//		campos.add("CTELCOMR");
//		campos.add("CTELBAIC");
//		campos.add("CTELNCEP");
//		campos.add("CTELCIDC");
//		campos.add("CTELUFED");
//
//		campos_valida_cond.add("CTELCOND");
//		campos_valida_sig.add("CTELTSIG");
//		String num_tel_pre="0";
//		String ddd="00"+telefone.trim().substring(0,2);
//		if (telefone.trim().substring(2).equals(9)){
//			num_tel_pre="";
//		};
//		String num_tel=num_tel_pre+telefone.trim().substring(2);
//		String telefone_tratado=ddd+num_tel;
//
//		try {
//			dataClient = BigtableDataClient.create(bigtableconfig.getProjectId(), bigtableconfig.getInstanceId());
//		} catch (IOException e) {
//			LOGGER.error("Ocorreu erro ao solicitar conexao com BigTable {}", e);
//		}
//
//		Filters.Filter filter = FILTERS.limit().cellsPerColumn(1);
//		List<Map<String, Object>> resultList = new ArrayList<>();
//
//		try {
//			String telefoneHash = Util.md5Hash(telefone_tratado);
//			Query query = Query.create(tableName).prefix(telefoneHash).filter(filter).limit(limit);
//
//			ServerStream<Row> rows = dataClient.readRows(query);
//			AtomicBoolean inibicao_a = new AtomicBoolean(false);
//			AtomicBoolean inibicao_s = new AtomicBoolean(false);
//
//			if (rows != null) {
//				LOGGER.info("################ Achou dados na tabela de SCPC FONE para telefone -> {}", telefoneHash);
//				rows.forEach(r -> {
//					final Map<String, Object> row = new HashMap<>();
//					r.getCells().forEach(cell -> {
//						String coluna_verifica = cell.getQualifier().toStringUtf8().toUpperCase().trim();
//						if (campos.indexOf(coluna_verifica) != -1){
//							row.put(coluna_verifica,cell.getValue().toStringUtf8());
//						}
//						if (campos_valida_cond.indexOf(coluna_verifica) != -1){
//							if (!cell.getValue().toStringUtf8().trim().equals("A")){
//								inibicao_a.set(true);
//							}
//						}
//						if (campos_valida_sig.indexOf(coluna_verifica) != -1){
//							if (cell.getValue().toStringUtf8().trim().equals("1")){
//								inibicao_s.set(true);
//							}
//						}
//					});
//					if (inibicao_a.get() == false){
//						if (inibicao_s.get() == true){
//							Map<String, Object> rowInibido   = new HashMap<>();
//							rowInibido.put("CTELTSIG","Endereço com flag de sigilo.");
//							resultList.add(rowInibido);
//						}
//						else {
//							resultList.add(row);
//						}
//					}
//				});
//			}
//		} catch (NotFoundException e) {
//			LOGGER.error("Tabela nao encontrada {}", e);
//		} catch (NullPointerException e) {
//			LOGGER.error("Fim da consulta {}", e);
//			return Optional.of(Collections.emptyList());
//		}
//
//		final Instant ended = Instant.now();
//        printLogger(LOGGER, "SCPC Fone", "scpc fone", telefone, ended);
//
//        dataClient.close();
//
//		return Optional.of(resultList);
//
//    }

	public Optional<List<Map<String, Object>>> scanTableOriginal(final String telefone) {

		String tableName = "base_cadastral_scpc_fone_produto";

		final Instant started = Instant.now();
		printLogger(LOGGER, "SCPC Fone", "scpc fone", telefone, started);

		long limit=1L;
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

		campos_valida_cond.add("CTELCOND");
		campos_valida_sig.add("CTELTSIG");
		String num_tel_pre="0";
		String ddd="00"+telefone.trim().substring(0,2);
		if (telefone.trim().substring(2).equals(9)){
			num_tel_pre="";
		};
		String num_tel=num_tel_pre+telefone.trim().substring(2);
		String telefone_tratado=ddd+num_tel;

		Filters.Filter filter = FILTERS.limit().cellsPerColumn(1);
		List<Map<String, Object>> resultList = new ArrayList<>();

		try {
			Map<String, Object> row = new HashMap<>();
			String telefoneHash = Util.md5Hash(telefone_tratado) + "-0";
			Row r = this.bigtableconfig.buildConnection().readRow(tableName, telefoneHash);

			AtomicBoolean inibicao_a = new AtomicBoolean(false);
			AtomicBoolean inibicao_s = new AtomicBoolean(false);

			for (RowCell cell : r.getCells()) {
				String column = cell.getQualifier().toStringUtf8().toUpperCase().trim();
				String value = cell.getValue().toStringUtf8();

				if (campos.indexOf(column) != -1 ) {
					row.put(column, value.trim());
				}
				if (campos_valida_cond.indexOf(column) != -1) {
					if (!value.trim().equals("A")) {
						inibicao_a.set(true);
					}
				}
				if (campos_valida_sig.indexOf(column) != -1) {
					if (value.trim().equals("1")) {
						inibicao_s.set(true);
					}
				}
			}

			if (r.getCells().size() > 0 && !inibicao_a.get() && !inibicao_s.get()) {
				resultList.add(row);
			}
		} catch (NotFoundException e) {
			LOGGER.error("Tabela nao encontrada {}", e);
		} catch (NullPointerException e) {
			//LOGGER.error("Fim da consulta {}", e);
			//return Optional.of(Collections.emptyList());
			printLogger(LOGGER, "fim da consulta", "scpcFone", md5Hash(telefone_tratado), started);
		} catch (IOException ioException) {
			LOGGER.error("Erro ao executar consulta: {}", ioException);
			return Optional.of(Collections.emptyList());
		}

		final Instant ended = Instant.now();
		printLogger(LOGGER, "SCPC Fone", "scpc fone", telefone, ended);
		return Optional.of(resultList);

	}

//    private List<String> orderResults(final Set<String> resultList, final String telefone) {
//        if(resultList == null || resultList.isEmpty())
//            return Collections.emptyList();
//
//        final Instant startedOrder = Instant.now();
//		printLogger(LOGGER, "bigTableScpcFondormOrder", "scpc fone", telefone, startedOrder);
//        List<String> sorted = resultList.stream().sorted().collect(Collectors.toList());
//		final Instant endedOrder = Instant.now();
//        printLogger(LOGGER, "bigTableScpcFondormOrder", "scpc fone", telefone, endedOrder);
//
//        return sorted;
//    }


}
