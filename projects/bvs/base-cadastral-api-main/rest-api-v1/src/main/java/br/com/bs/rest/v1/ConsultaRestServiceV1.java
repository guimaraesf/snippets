package br.com.bs.rest.v1;

import br.com.bs.config.AppConfig;
import br.com.bs.exception.ApiException;
import br.com.bs.interfaces.DatabaseScan;
import br.com.bs.util.CadastralUtil;
import br.com.bs.util.service.ConsultaRestServiceBase;
import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static br.com.bs.config.Constants.*;
import static br.com.bs.util.Util.*;
import static java.util.Map.Entry;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toMap;

@Primary
@Service
public class ConsultaRestServiceV1 extends ConsultaRestServiceBase {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
    private final AppConfig config;
    private final DatabaseScan databaseScan;
    private final CadastralUtil cadastralUtil;

    @Autowired
    public ConsultaRestServiceV1(AppConfig config, DatabaseScan databaseScan, CadastralUtil cadastralUtil) {
        this.config = config;
        this.databaseScan = databaseScan;
        this.cadastralUtil = cadastralUtil;
    }

    private CompletableFuture<Map<String, Object>> getBigTableNomeAsync(Exchange exchng, Map<String, String> requestMap){
        return CompletableFuture.supplyAsync(() -> {
            final Optional result = executeBigTable(exchng, C_NOME, requestMap);
            return cadastralUtil.findNome(result);
        });
    }

    private CompletableFuture<Map<String, Object>> getBigTablePessoaRelacionadaAsync(Exchange exchng, Map<String, String> requestMap){
        return CompletableFuture.supplyAsync(() -> {
            final Optional result = executeBigTableList(exchng, C_PESSOARELACIONADA, requestMap);
            return cadastralUtil.findPessoaRelacionada(result);
        });
    }

    private CompletableFuture<Map<String, Object>> getBigTableAsync(Exchange exchng, String contexto, Map<String, String> requestMap){
        return CompletableFuture.supplyAsync(() -> {
            final Optional result = executeBigTable(exchng, contexto, requestMap);
            return (Map<String, Object>)performDeParaCampos(cadastralUtil.checkDefaultSituacaoReceita(contexto, result), contexto, config);
        });
    }

    public void findIdentificacaoProdutoTable(Exchange exchng) throws Exception{
        exchng.getIn().setHeader("contexto", "v1-identificacao");
        Map<String, Object> nomeResult = cadastralUtil.findNome(executeBigTable(exchng, C_NOME));
        if (nomeResult.isEmpty()) {
            writeResponse(exchng, null, of(Collections.emptyMap()), HashMap.class);
            return;
        }
        //quando entrar menor de idade nos contextos de v1, descomentar as linhas abaixo
        /*final Object first = cadastralUtil.validaSemResultadosGetFirst(nomeResult, CpfCnpjNaoEncontradoException::new);
        Map<String, Object> map = first instanceof Map ? (Map<String, Object>) first : new HashMap<>();

        int menorIdade = isMenorIdade(map.getOrDefault(config.getColumnQualifier().getDataNascimento(), "").toString());
        if ( menorIdade == 0) {
            throw new CpfCnpjMenorIdadeException();
        } else if (menorIdade == 2) {*/
            final Map<String, String> requestMap = readRequestHeaders(exchng.getIn());

            List<CompletableFuture<Map<String, Object>>> contextos = new ArrayList<CompletableFuture<Map<String, Object>>>() {{
                add(getBigTablePessoaRelacionadaAsync(exchng, requestMap));
                add(getBigTableAsync(exchng, C_GRAUINSTRUCAO, requestMap));
                add(getBigTableAsync(exchng, C_CLASSESOCIAL, requestMap));
                add(getBigTableAsync(exchng, C_NOMEPAIS, requestMap));
                add(getBigTableAsync(exchng, C_NACIONALIDADE, requestMap));
                add(getBigTableAsync(exchng, C_NATURALIDADE, requestMap));
                add(getBigTableAsync(exchng, C_ESTADOCIVIL, requestMap));
                add(getBigTableAsync(exchng, C_SEXO, requestMap));
                add(getBigTableAsync(exchng, C_OCUPACAO, requestMap));
                add(getBigTableAsync(exchng, C_DEPENDENTES, requestMap));
                add(getBigTableAsync(exchng, C_ANO_OBITO, requestMap));
                add(getBigTableAsync(exchng, C_PROTOCOLO, requestMap));
            }};
            Map<String, Object> asyncResult =
                    contextos.stream()
                            .map(CompletableFuture::join)
                            .flatMap((e) -> e.entrySet().stream())
                            .collect( toMap(Entry::getKey, Entry::getValue, (p1, p2) -> p1) );

            asyncResult.putAll(nomeResult);

            writeResponse(exchng, null, of(asyncResult), HashMap.class);
        /*} else {
            final Map<String, String> requestMap = readRequestHeaders(exchng.getIn());
            List<CompletableFuture<Map<String, Object>>> contextos = new ArrayList<CompletableFuture<Map<String, Object>>>() {{
                add(getBigTableAsync(exchng, C_PROTOCOLO, requestMap));
            }};
            Map<String, Object> asyncResult =
                    contextos.stream()
                            .map(CompletableFuture::join)
                            .flatMap((e) -> e.entrySet().stream())
                            .collect( toMap(Entry::getKey, Entry::getValue, (p1, p2) -> p1) );

            asyncResult.putAll(nomeResult);

            writeResponse(exchng, null, of(asyncResult), HashMap.class);
        }*/
    }

    public void findContatoProdutoTable(Exchange exchng) throws Exception{
        if (cadastralUtil.findNome(executeBigTable(exchng, C_NOME)).isEmpty()) {
            writeResponse(exchng, null, of(Collections.emptyList()), ArrayList.class);
            return;
        }

        //quando entrar validacao de menor de idade, remover comentarios abaixo e comentar linhas acima
        /*Map<String, Object> nomeResult = cadastralUtil.findNome(executeBigTable(exchng, C_NOME));
        if (nomeResult.isEmpty()) {
            writeResponse(exchng, null, of(Collections.emptyMap()), HashMap.class);
            return;
        }
        final Object first = cadastralUtil.validaSemResultadosGetFirst(nomeResult, CpfCnpjNaoEncontradoException::new);
        cadastralUtil.validaEhMenorDeIdade(first, C_TELEFONE);*/

        Optional result = executeBigTableList(exchng, C_TELEFONE);
        List<Map> contatosList = cadastralUtil.findTelefonesFixosECelulares(result, true);
        writeResponse(exchng, C_TELEFONE, of(contatosList), ArrayList.class);
    }

    public void findDependenteProdutoTable(Exchange exchng) throws Exception{
        if (cadastralUtil.findNome(executeBigTable(exchng, C_NOME)).isEmpty()) {
            writeResponse(exchng, null, of(Collections.emptyList()), ArrayList.class);
            return;
        }

        //quando entrar validacao de menor de idade, remover comentarios abaixo e comentar linhas acima
        /*Map<String, Object> nomeResult = cadastralUtil.findNome(executeBigTable(exchng, C_NOME));
        if (nomeResult.isEmpty()) {
            writeResponse(exchng, null, of(Collections.emptyMap()), HashMap.class);
            return;
        }
        final Object first = cadastralUtil.validaSemResultadosGetFirst(nomeResult, CpfCnpjNaoEncontradoException::new);
        cadastralUtil.validaEhMenorDeIdade(first, C_DEPENDENTES);*/

        Optional result = executeBigTableList(exchng, C_DEPENDENTES);
        writeResponse(exchng, C_DEPENDENTES, result, ArrayList.class);
    }

    public void findOcupacaoProdutoTable(Exchange exchng) throws Exception{
        if (cadastralUtil.findNome(executeBigTable(exchng, C_NOME)).isEmpty()) {
            writeResponse(exchng, null, of(Collections.emptyList()), ArrayList.class);
            return;
        }

        //quando entrar validacao de menor de idade, remover comentarios abaixo e comentar linhas acima
        /*Map<String, Object> nomeResult = cadastralUtil.findNome(executeBigTable(exchng, C_NOME));
        if (nomeResult.isEmpty()) {
            writeResponse(exchng, null, of(Collections.emptyMap()), HashMap.class);
            return;
        }
        final Object first = cadastralUtil.validaSemResultadosGetFirst(nomeResult, CpfCnpjNaoEncontradoException::new);
        cadastralUtil.validaEhMenorDeIdade(first, C_OCUPACAO);*/

        Optional result = executeBigTableList(exchng, C_OCUPACAO);
        writeResponse(exchng, C_OCUPACAO, result, ArrayList.class);
    }

    public void findEndInstProdutoTable(Exchange exchng) throws Exception{
        if (cadastralUtil.findNome(executeBigTable(exchng, C_NOME)).isEmpty()) {
            writeResponse(exchng, null, of(Collections.emptyList()), ArrayList.class);
            return;
        }

        //quando entrar validacao de menor de idade, remover comentarios abaixo e comentar linhas acima
        /*Map<String, Object> nomeResult = cadastralUtil.findNome(executeBigTable(exchng, C_NOME));
        if (nomeResult.isEmpty()) {
            writeResponse(exchng, null, of(Collections.emptyMap()), HashMap.class);
            return;
        }
        final Object first = cadastralUtil.validaSemResultadosGetFirst(nomeResult, CpfCnpjNaoEncontradoException::new);
        cadastralUtil.validaEhMenorDeIdade(first, C_ENDINST);*/

        Optional result = executeBigTableList(exchng, C_ENDINST);
        writeResponse(exchng, C_ENDINST, result, ArrayList.class);
    }

    public void findDocumentoProdutoTable(Exchange exchng) throws Exception{
        if (cadastralUtil.findNome(executeBigTable(exchng, C_NOME)).isEmpty()) {
            writeResponse(exchng, null, of(Collections.emptyList()), ArrayList.class);
            return;
        }

        //quando entrar validacao de menor de idade, remover comentarios abaixo e comentar linhas acima
        /*Map<String, Object> nomeResult = cadastralUtil.findNome(executeBigTable(exchng, C_NOME));
        if (nomeResult.isEmpty()) {
            writeResponse(exchng, null, of(Collections.emptyMap()), HashMap.class);
            return;
        }
        final Object first = cadastralUtil.validaSemResultadosGetFirst(nomeResult, CpfCnpjNaoEncontradoException::new);
        cadastralUtil.validaEhMenorDeIdade(first, C_DOCUMENTO);*/

        final Map<String, String> requestMap = readRequestHeaders(exchng.getIn());
        Optional result = executeBigTableList(exchng, C_DOCUMENTO, requestMap);
        writeResponse(exchng, C_DOCUMENTO, cadastralUtil.aplicaFiltroDocumento(requestMap, result), ArrayList.class);
    }

    public void findObitoProdutoTable(Exchange exchng) throws Exception{
        if (cadastralUtil.findNome(executeBigTable(exchng, C_NOME)).isEmpty()) {
            writeResponse(exchng, null, of(Collections.emptyMap()), HashMap.class);
            return;
        }

        Optional resultFind = executeBigTable(exchng, C_ANO_OBITO);

        Map<String, Object> map = new HashMap<>();
        map.put("FLAG_OBITO", resultFind.isPresent() ? 1 : 0);

        Optional result =  Optional.of(map);

        writeResponse(exchng, C_ANO_OBITO, result, HashMap.class);
    }

    public void findPPEProdutoTable(Exchange exchng) throws Exception{
        if (cadastralUtil.findNome(executeBigTable(exchng, C_NOME)).isEmpty()) {
            writeResponse(exchng, null, of(Collections.emptyMap()), HashMap.class);
            return;
        }

        Optional resultFind = executeBigTable(exchng, C_PPE);

        Map<String, Object> map = new HashMap<>();
        map.put("FLAG_PPE", resultFind.isPresent() ? 1 : 0);

        Optional result =  Optional.of(map);
        writeResponse(exchng, C_PPE, result, HashMap.class);
    }

    public void findEnderecoProdutoTable(Exchange exchng) throws Exception{
        if (cadastralUtil.findNome(executeBigTable(exchng, C_NOME)).isEmpty()) {
            writeResponse(exchng, null, of(Collections.emptyList()), ArrayList.class);
            return;
        }

        //quando entrar validacao de menor de idade, remover comentarios abaixo e comentar linhas acima
        /*Map<String, Object> nomeResult = cadastralUtil.findNome(executeBigTable(exchng, C_NOME));
        if (nomeResult.isEmpty()) {
            writeResponse(exchng, null, of(Collections.emptyMap()), HashMap.class);
            return;
        }
        final Object first = cadastralUtil.validaSemResultadosGetFirst(nomeResult, CpfCnpjNaoEncontradoException::new);
        cadastralUtil.validaEhMenorDeIdade(first, C_ENDERECO);*/

        Optional result = executeBigTableList(exchng, C_ENDERECO);

        List<Map<String, Object>> enderecos = (List<Map<String, Object>>)result.orElse(new ArrayList<Map>());
        cadastralUtil.findEndereco(enderecos);

        writeResponse(exchng, C_ENDERECO, of(enderecos), ArrayList.class);
    }

    private Optional executeBigTable(final Exchange exchng, final String contexto){
        final Map<String, String> requestMap = readRequestHeaders(exchng.getIn());
        return executeBigTable(exchng, contexto,  requestMap);
    }

    private Optional executeBigTable(final Exchange exchng, final String contexto, Map<String, String> requestMap){
        configureMDC(exchng);
        return databaseScan.findProdutoTable(contexto, requestMap,  config.getContextoFetchColumns(contexto));
    }

    private Optional executeBigTableList(final Exchange exchng, final String contexto){
        final Map<String, String> requestMap = readRequestHeaders(exchng.getIn());
        return executeBigTableList(exchng, contexto, requestMap);
    }

    private Optional executeBigTableList(final Exchange exchng, final String contexto, Map<String, String> requestMap){
        configureMDC(exchng);
        return databaseScan.findProdutoTableList(contexto, requestMap,  config.getContextoFetchColumns(contexto));
    }

    private void writeResponse(final Exchange exchng, String contexto, Optional result, Class<?> clazz){
        try {
            exchng.getOut().setBody(performDeParaCampos(result.orElseGet(ArrayList::new), contexto, config), clazz);
        } catch (Exception e) {
            LOGGER.error("Erro ao montar resposta para dados não encontrados para o CPF: ", e);
            throw  new ApiException(e.getMessage());
        }
    }

}
