package br.com.bs.rest.v2;

import br.com.bs.config.AppConfig;
import br.com.bs.exception.ApiException;
import br.com.bs.exception.CpfCnpjAbstractException;
import br.com.bs.interfaces.*;
import br.com.bs.ppe.application.service.PPEPessoaService;
import br.com.bs.util.CadastralUtil;
import br.com.bs.util.Util;
import br.com.bs.util.builders.TipoFiltro;
import br.com.bs.util.service.ConsultaRestServiceBase;
import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static br.com.bs.config.Constants.C_NOME;
import static br.com.bs.config.Constants.C_PESSOARELACIONADA;
import static br.com.bs.util.Util.configureMDC;

@SuppressWarnings("ALL")
@Service
public class ConsultaRestServiceV2 extends ConsultaRestServiceBase {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private final AppConfig appConfig;
    private final DatabaseScan databaseScan;
    private final DatabaseReversedIndexScan databaseReversedIndexScan;
	private final DatabaseScpcFoneScan databaseScpcFoneScan;
	private final DatabasePpePessoaScan databasePpeScan;
	private final DatabaseEnderecoCepScan databaseEnderecoCepScan;
	private final DatabaseOutrasGrafiasScan databaseOutrasGrafiasScan;
    private final CadastralUtil cadastralUtil;
    private final PPEPessoaService ppePessoaService;

    @Autowired
    public ConsultaRestServiceV2(AppConfig appConfig,
                                 DatabaseScan databaseScan,
                                 DatabaseReversedIndexScan databaseReversedIndexScan,
                                 DatabaseScpcFoneScan databaseScpcFoneScan,
                                 DatabasePpePessoaScan databasePpeScan,
                                 DatabaseEnderecoCepScan databaseEnderecoCepScan,
                                 DatabaseOutrasGrafiasScan databaseOutrasGrafiasScan,
                                 CadastralUtil cadastralUtil,
                                 PPEPessoaService ppePessoaService) {
        this.appConfig = appConfig;
        this.databaseScan = databaseScan;
        this.databaseReversedIndexScan = databaseReversedIndexScan;
        this.databaseScpcFoneScan = databaseScpcFoneScan;
        this.databasePpeScan = databasePpeScan;
        this.databaseEnderecoCepScan = databaseEnderecoCepScan;
        this.databaseOutrasGrafiasScan = databaseOutrasGrafiasScan;
        this.cadastralUtil = cadastralUtil;
        this.ppePessoaService = ppePessoaService;
    }

    public void findReversedIndexTable(Exchange exchng) throws Exception{
        final Map<String, String> requestHeaders = readRequestHeaders(exchng.getIn());

        final String origem = requestHeaders.get("origem").toUpperCase();
        final String verbo = requestHeaders.get("verbo").toLowerCase();

        Supplier<Optional> function = requestHeaders.containsKey("destino") ?
                () -> databaseReversedIndexScan.findReverdIndexTable(origem, verbo, requestHeaders.get("destino").toUpperCase() ) :
                () -> databaseReversedIndexScan.findReverdIndexTable(origem, verbo );

        executeContextoTable(exchng, function, ArrayList.class);
    }


    public void findScpcFoneTable(Exchange exchng) throws Exception{
		final String telefone = exchng.getIn().getHeader("telefone", String.class);

        boolean destino = headerPresentOrTrue("destino", exchng.getIn());

        Supplier<Optional> function = destino ?
			() -> null :
			() -> databaseScpcFoneScan.findScpcFonTable(telefone);

        executeContextoTable(exchng, function, ArrayList.class);
    }

    public void findContextoProdutoTable(Exchange exchng) throws Exception{
        final String contexto = exchng.getIn().getHeader("contexto", String.class);

        boolean buscaCompleta = headerPresentOrTrue("busca_completa", exchng.getIn());
        Supplier<Optional> function = buscaCompleta ?
                () -> databaseScan.findFullProdutoTableList(contexto, readRequestHeaders(exchng.getIn())) :
                () -> this.findProduto(contexto, readRequestHeaders(exchng.getIn()), buscaCompleta);

        executeContextoTable(contexto, buscaCompleta, exchng, function, ArrayList.class);
    }

    private Optional findProduto(final String contexto, final Map<String, String> headers, final boolean buscaCompleta ) {
        final Optional<List<Map<String, Object>>> produtoTableList = databaseScan.findProdutoTableList(contexto, headers);
        if (C_PESSOARELACIONADA.equals(contexto) && !buscaCompleta)
            enriquecerDataNascimento(produtoTableList);
        return produtoTableList;
    }

    private void enriquecerDataNascimento(final Optional<List<Map<String, Object>>> produtoTableList) {
        final String campoDtNasc = appConfig.getColumnQualifier().getDataNascimento();
        final String campoCpfRelacionado = appConfig.getColumnQualifier().getCpfRelacionado();

        produtoTableList.ifPresent( list -> list.parallelStream()
            .filter( item -> item.containsKey(campoCpfRelacionado) && !item.get(campoCpfRelacionado).toString().equals(""))
            .forEach( item -> {
                final Map<String, String> filtroRelacionado = new HashMap<String, String>() {{
                    put(TipoFiltro.paramFiltro(TipoFiltro.CPF), item.get(campoCpfRelacionado).toString());
                }};
                databaseScan.findProdutoTable(C_NOME, filtroRelacionado, campoDtNasc).ifPresent(item::putAll);
            }));
    }

    private void executeContextoTable(final Exchange exchng, final Supplier<Optional> function, Class resultClass){
        executeContextoTable(null, false, exchng, function, resultClass);
    }

    private void executeContextoTable(final String contexto, final boolean buscaCompleta, final Exchange exchng, final Supplier<Optional> function, Class resultClass){
        configureMDC(exchng);
        try {
            final Object result = Util.removeBlankOrNull(function.get().orElse(resultClass.newInstance()));
            final Map<String, String> requestMap = readRequestHeaders(exchng.getIn());
            cadastralUtil.validaExceptionsCadastral(result, contexto, buscaCompleta, requestMap);
            exchng.getOut().setBody(result, resultClass);
        }catch (CpfCnpjAbstractException e){
            throw e;
        } catch (Exception e) {
            LOGGER.error("Erro ao montar resposta para dados não encontrados para o CPF: ", e);
            throw new ApiException(e.getMessage());
        }
    }

    public void ppe(Exchange exchng) throws Exception {
        try {
            final String cpf = exchng.getIn().getHeader("cpf", String.class);
            configureMDC(exchng);
            final Object result = Util.removeBlankOrNull(this.ppePessoaService.findByCPFMap(cpf));
            exchng.getOut().setBody(result, Map.class);
        } catch (CpfCnpjAbstractException e) {
            throw e;
        } catch (Exception e) {
            LOGGER.error("Erro ao montar resposta para dados não encontrados para o CPF: ", e);
            throw new ApiException(e.getMessage());
        }
    }

    public void ppe_pessoa(Exchange exchng) throws Exception{
        final String cpf = exchng.getIn().getHeader("cpf", String.class);

        boolean destino = headerPresentOrTrue("destino", exchng.getIn());

        Supplier<Optional> function = destino ?
                () -> null :
                () -> databasePpeScan.ppe_pessoa(cpf);

        executeContextoTable(exchng, function, ArrayList.class);
    }

    public void ppe_pessoa_nome(Exchange exchng) throws Exception{
        final String nome = exchng.getIn().getHeader("nome", String.class);

        boolean destino = headerPresentOrTrue("destino", exchng.getIn());

        Supplier<Optional> function = destino ?
                () -> null :
                () -> databasePpeScan.ppe_pessoa_nome(nome);

        executeContextoTable(exchng, function, ArrayList.class);
    }

    public void ppe_pessoa_mandato(Exchange exchng) throws Exception{
        final String cpf = exchng.getIn().getHeader("cpf", String.class);

        boolean destino = headerPresentOrTrue("destino", exchng.getIn());

        Supplier<Optional> function = destino ?
                () -> null :
                () -> databasePpeScan.ppe_pessoa_mandato(cpf);

        executeContextoTable(exchng, function, ArrayList.class);
    }

    public void ppe_pessoa_relacionados(Exchange exchng) throws Exception{
        final String cpf = exchng.getIn().getHeader("cpf", String.class);

        boolean destino = headerPresentOrTrue("destino", exchng.getIn());

        Supplier<Optional> function = destino ?
                () -> null :
                () -> databasePpeScan.ppe_pessoa_relacionados(cpf);

        executeContextoTable(exchng, function, ArrayList.class);
    }

    public void ppe_pessoa_titular(Exchange exchng) throws Exception{
        final String cpf = exchng.getIn().getHeader("cpf", String.class);

        boolean destino = headerPresentOrTrue("destino", exchng.getIn());

        Supplier<Optional> function = destino ?
                () -> null :
                () -> databasePpeScan.ppe_pessoa_titular(cpf);

        executeContextoTable(exchng, function, ArrayList.class);
    }

    public void ppe_pessoa_titular_relacionado(Exchange exchng) throws Exception {
        final Map<String, String> requestHeaders = readRequestHeaders(exchng.getIn());

        final String cpf_titular = requestHeaders.get("cpf_titular").toUpperCase();
        final String cpf_relacionado = requestHeaders.get("cpf_relacionado").toLowerCase();


        Supplier<Optional> function = requestHeaders.containsKey("destino") ?
                () -> null :
                () -> databasePpeScan.ppe_pessoa_titular_relacionado(cpf_titular, cpf_relacionado);

        executeContextoTable(exchng, function, ArrayList.class);
    }

    public void endereco_cep(Exchange exchng) throws Exception{
        final String cep = exchng.getIn().getHeader("cep", String.class);

        boolean destino = headerPresentOrTrue("destino", exchng.getIn());

        Supplier<Optional> function = destino ?
                () -> null :
                () -> databaseEnderecoCepScan.endereco_cep(cep);

        executeContextoTable(exchng, function, ArrayList.class);
    }


    public void outras_grafias(Exchange exchng) throws Exception{
        final String cpf = exchng.getIn().getHeader("cpf", String.class);

        boolean destino = headerPresentOrTrue("destino", exchng.getIn());

        Supplier<Optional> function = destino ?
                () -> null :
                () -> databaseOutrasGrafiasScan.outras_grafias(cpf);

        executeContextoTable(exchng, function, ArrayList.class);
    }
}
