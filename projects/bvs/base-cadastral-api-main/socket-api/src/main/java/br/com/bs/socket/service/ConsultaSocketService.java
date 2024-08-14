package br.com.bs.socket.service;

import br.com.bs.config.AppConfig;
import br.com.bs.exception.ApiException;
import br.com.bs.interfaces.DatabaseScan;
import br.com.bs.socket.config.SocketConfig;
import br.com.bs.socket.data.RequestData;
import br.com.bs.socket.data.ResponseData;
import br.com.bs.util.CadastralUtil;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.component.netty4.NettyConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static br.com.bs.config.Constants.*;
import static br.com.bs.util.Util.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@Profile("socket")
@Service
public class ConsultaSocketService {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
    private final AppConfig config;
    private final SocketConfig socketConfig;
    private final DatabaseScan databaseScan;
    private final CadastralUtil cadastralUtil;
    //quando validacao de menor de idade entrar nas consultas via socket, descomentar linha abaixo
    //private final ConsultaRestServiceBase consultaRestServiceBase;

    private static final int QUANTIDADE_TELEFONE_GOLDEN = 3;

    @Autowired
    public ConsultaSocketService(AppConfig config, SocketConfig socketConfig, DatabaseScan databaseScan, CadastralUtil cadastralUtil) {
        this.config = config;
        this.socketConfig = socketConfig;
        this.databaseScan = databaseScan;
        this.cadastralUtil = cadastralUtil;
        //quando validacao de menor de idade entrar nas consultas via socket, descomentar linha abaixo e incluir
        //argumento no metodo
        //this.consultaRestServiceBase = consultaRestServiceBase;
    }

    public void findProdutoTable(Exchange exchng) throws Exception {
        String body = exchng.getIn().getBody(String.class);
        String ipExterno = exchng.getIn().getHeader("CamelNettyRemoteAddress", String.class);

        if (body.equals(socketConfig.getByeCommand())) {
            exchng.getOut().setHeader(NettyConstants.NETTY_CLOSE_CHANNEL_WHEN_COMPLETE, true);
            return;
        }

        RequestData request;
        String transacao = StringUtils.substring(body, 0, 8).trim();
        //String logBody = body.replaceAll("CPF([0-9]*)\\w+","CPF".concat(md5Hash("000".concat("$1"))));

        try {
            LOGGER.info("[SOCKET] Mensagem recebida - Body: {}", body);
            request = RequestData.parse(body, exchng.getCreated(), ipExterno);
            //RequestData logRequest = request;
            //logRequest.setDocumento(md5Hash(request.getDocumento()));
            LOGGER.info("[SOCKET] Mensagem recebida - Parsed: {}", request);
            //LOGGER.info("[SOCKET] Documento recebido - {}", request.getDocumento());
        } catch (Exception e) {
            LOGGER.error("Mensagem em formato inválido", e);
            ResponseData response = ResponseData.parse(null, "", transacao, socketConfig);
            exchng.getOut().setBody(response.toString(), String.class);

            final Instant finished = Instant.now();
            LOGGER.info("cpf={} statuscode={} started={} finished={} elapsedtime={} ip={} service={}",
                    "INVALIDO", 400, getDateFormatted(exchng.getCreated()), getDateFormatted(finished),
                    Duration.between(exchng.getCreated().toInstant(), finished).toMillis(),
                    ipExterno, "INVALIDO");
            return;
        }

        try {
            switch (transacao) {
                case "HEALTHCH": //Health Check
                    LOGGER.info("Health Check Socket");
                    exchng.getOut().setBody("UP", String.class);
                    break;
                //DOCUMENTO
                case "BMACMDOC": //Melhor Documento CPF Tipo-SPMD_MELHOR_DOCUMENTO_POR_CPF_TIPO
                    executeBigTableDocumentoTipoList(exchng, C_DOCUMENTO, request);
                    break;
                case "BMIDOCNT": //Documento por CPF origem-SPMD_DOCUMENTO_POR_CPF_ORIGEM
                    executeBigTableList(exchng, C_DOCUMENTO, request);
                    break;
                //IDENTIFICACAO + OBITO
                case "BMIDEN00": //Acesso Melhor Identificação + óbito-SPMD_MELHOR_IDENTIFICACAO_POR_CPF e SPMD_OBITO_POR_CPF
                    findIdentificacao(exchng, request, true, false);
                    break;
                //IDENTIFICACAO
                case "BMACIDEN": //Acesso Melhor Identificação-SPMD_MELHOR_IDENTIFICACAO_POR_CPF
                    findIdentificacao(exchng, request, false, false);
                    break;
                case "BMIDECNT": //Identificação por CPF-SPMD_IDENTIFICACAO_POR_CPF
                    findIdentificacao(exchng, request, false, true);
                    break;
                //CONTATO
                case "BMACMCNT": //Melhor Contato Por CPF-SPMD_MELHOR_CONTATO_POR_CPF
                    executeBigTableTelefonesList(exchng, C_TELEFONE, request, QUANTIDADE_TELEFONE_GOLDEN);
                    break;
                case "BMACCONT": //Contato via Documento-SPMD_CONTATO_POR_CPF
                    executeBigTableTelefonesList(exchng, C_TELEFONE, request);
                    break;
                //ENDERECO
                case "BMACMEND": //Acesso Melhor Endereço por CPF-SPMD_MELHOR_ENDERECO_POR_CPF
                    executeBigTableEnderecosList(exchng, C_ENDERECO, request);
                    break;
                case "BMACENDE": //Acesso Endereço por Origem CPF-SPMD_ENDERECO_POR_CPF_ORIGEM
                case "BMBPFEND": //Busca endereço por CPF-SPMD_ENDERECO_POR_CPF
                    executeBigTableEnderecosList(exchng, C_ENDERECO, request);
                    break;
                default:
                    String msg = "Serviço inválido na mensagem. Disponíveis: BMACMDOC, BMIDOCNT, BMIDEN00, BMACIDEN, BMIDECNT, BMACMCNT, BMACCONT, BMACMEND, BMACENDE, BMBPFEND";
                    LOGGER.error(msg);
                    throw new ApiException(HttpStatus.NOT_FOUND.value(), msg);
            }
        } catch (ApiException e) {
            ResponseData response = ResponseData.parse(null, request.getDocumento(), transacao, socketConfig);
            exchng.getOut().setBody(response.toString(), String.class);

            final Instant finished = Instant.now();
            LOGGER.info("cpf={} statuscode={} started={} finished={} elapsedtime={} ip={} service={}",
                    "INVALIDO", 500, getDateFormatted(exchng.getCreated()), getDateFormatted(finished),
                    Duration.between(exchng.getCreated().toInstant(), finished).toMillis(),
                    request.getIpExterno(), "INVALIDO");
            return;
        }
    }

    private CompletableFuture<Map<String, Object>> getBigTableNomeAsync(Exchange exchng, Map<String, String> requestMap) {
        return CompletableFuture.supplyAsync(() -> {
            final Optional result = databaseScan.findProdutoTable(C_NOME, requestMap, config.getContextoFetchColumns(C_NOME));
            return cadastralUtil.findNome(result);
        });
    }

    private CompletableFuture<Map<String, Object>> getBigTablePessoaRelacionadaAsync(Exchange exchng, Map<String, String> requestMap) {
        return CompletableFuture.supplyAsync(() -> {
            final Optional result = databaseScan.findProdutoTableList(C_PESSOARELACIONADA, requestMap, config.getContextoFetchColumns(C_PESSOARELACIONADA));
            return cadastralUtil.findPessoaRelacionada(result);
        });
    }

    private CompletableFuture<Map<String, Object>> getBigTableAsync(String contexto, Map<String, String> requestMap) {
        return CompletableFuture.supplyAsync(() -> {
            final Optional result = databaseScan.findProdutoTable(contexto, requestMap, config.getContextoFetchColumns(contexto));
            return (Map<String, Object>) performDeParaCampos(cadastralUtil.checkDefaultSituacaoReceita(contexto, result), contexto, config);
        });
    }

    private void findIdentificacao(Exchange exchng, RequestData request, boolean findObito, boolean outrasGrafias) {
        final Map<String, String> requestMap = readRequestHeaders(request);
        requestMap.put("contexto", "socket-identificacao");

        Map<String, Object> nomeResult = cadastralUtil.findNome(databaseScan.findProdutoTable(C_NOME, requestMap, config.getContextoFetchColumns(C_NOME)));
        if (nomeResult.isEmpty()) {
            writeResponse(exchng.getOut(), Collections.emptyMap(), null, request);
            return;
        }

        //quando for implantar validacao de menor de idade no socket, retirar comentarios abaixo
        /*final Object first = cadastralUtil.validaSemResultadosGetFirst(nomeResult, CpfCnpjNaoEncontradoException::new);
        Map<String, Object> map = first instanceof Map ? (Map<String, Object>) first : new HashMap<>();

        int menorIdade = isMenorIdade(map.getOrDefault(config.getColumnQualifier().getDataNascimento(), "").toString());
        System.out.println("Retorno da funcao de menor de idade: " +menorIdade);
        if ( menorIdade == 0) {
            writeResponse(exchng.getOut(), Collections.emptyMap(), null, request);
            return;
        } else if (menorIdade == 2) {*/
            List<CompletableFuture<Map<String, Object>>> contextos = new ArrayList<CompletableFuture<Map<String, Object>>>() {{
                add(getBigTablePessoaRelacionadaAsync(exchng, requestMap));
                add(getBigTableAsync(C_GRAUINSTRUCAO, requestMap));
                add(getBigTableAsync(C_CLASSESOCIAL, requestMap));
                add(getBigTableAsync(C_NOMEPAIS, requestMap));
                add(getBigTableAsync(C_NACIONALIDADE, requestMap));
                add(getBigTableAsync(C_NATURALIDADE, requestMap));
                add(getBigTableAsync(C_ESTADOCIVIL, requestMap));
                add(getBigTableAsync(C_SEXO, requestMap));
                add(getBigTableAsync(C_OCUPACAO, requestMap));
                add(getBigTableAsync(C_DEPENDENTES, requestMap));
                add(getBigTableAsync(C_ANO_OBITO, requestMap));
                add(getBigTableAsync(C_PROTOCOLO, requestMap));
            }};

            Map<String, Object> result =
                    contextos.stream()
                            .map(CompletableFuture::join)
                            .flatMap((e) -> e.entrySet().stream())
                            .collect( toMap(Map.Entry::getKey, Map.Entry::getValue, (p1, p2) -> p1) );

            result.putAll(nomeResult);

            if (!result.isEmpty() && findObito) {
                result.put("COD_OBT", result.containsKey("ANO_OBTO") ? "S": "N");
            }

            if (outrasGrafias) {
                List<Map<String, Object>> resultOutrasGrafias = new ArrayList<>();
                List<Map<String, Object>> buscaEndereco = databaseScan.findProdutoTableList(C_ENDERECO, requestMap, config.getContextoFetchColumns(C_ENDERECO)).orElseGet(Collections::emptyList);
                buscaEndereco.forEach(e -> {
                    Map<String, Object> novoNome = result.entrySet().stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
                    novoNome.put("COD_ORGM", e.getOrDefault("COD_ORGM", ""));
                    novoNome.putAll(addCodPssFis(novoNome));
                    resultOutrasGrafias.add(novoNome);
                });
                writeResponse(exchng.getOut(), resultOutrasGrafias, C_NOME, request);
            } else {
                writeResponse(exchng.getOut(), result, null, request);
            }
        /*} else {
            //final Map<String, String> requestMap = readRequestHeaders(request);
            List<CompletableFuture<Map<String, Object>>> contextos = new ArrayList<CompletableFuture<Map<String, Object>>>() {{
                add(getBigTableAsync(C_PROTOCOLO, requestMap));
            }};

            Map<String, Object> result =
                    contextos.stream()
                            .map(CompletableFuture::join)
                            .flatMap((e) -> e.entrySet().stream())
                            .collect( toMap(Map.Entry::getKey, Map.Entry::getValue, (p1, p2) -> p1) );

            result.putAll(nomeResult);

            if (!result.isEmpty() && findObito) {
                result.put("COD_OBT", "N");
                databaseScan.findProdutoTable(C_OBITO, requestMap, config.getContextoFetchColumns(C_OBITO))
                        .ifPresent(o -> result.put("COD_OBT", "S"));
            }

            if (outrasGrafias) {
                List<Map<String, Object>> resultOutrasGrafias = new ArrayList<>();
                List<Map<String, Object>> buscaEndereco = databaseScan.findProdutoTableList(C_ENDERECO, requestMap, config.getContextoFetchColumns(C_ENDERECO)).orElseGet(Collections::emptyList);
                buscaEndereco.forEach(e -> {
                    Map<String, Object> novoNome = result.entrySet().stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
                    novoNome.put("COD_ORGM", e.getOrDefault("COD_ORGM", ""));
                    novoNome.putAll(addCodPssFis(novoNome));
                    resultOutrasGrafias.add(novoNome);
                });
                writeResponse(exchng.getOut(), resultOutrasGrafias, C_NOME, request);
            } else {
                writeResponse(exchng.getOut(), result, null, request);
            }
        }*/
    }

    private void executeBigTable(final Exchange exchng, final String contexto, final RequestData request) {
        final Map<String, String> requestMap = readRequestHeaders(request);
        executeBigTable(exchng, contexto, request, requestMap);
    }

    private void executeBigTable(final Exchange exchng, final String contexto, final RequestData request, final Map<String, String> requestMap) {
        Optional<Map<String, Object>> result = databaseScan.findProdutoTable(contexto, requestMap, config.getContextoFetchColumns(contexto));
        writeResponse(exchng.getOut(), result.orElseGet(Collections::emptyMap), contexto, request);
    }

    private void executeBigTableList(final Exchange exchng, final String contexto, final RequestData request, int limit) {
        final Map<String, String> requestMap = readRequestHeaders(request);

        //quando for implantar validacao de menor de idade no socket, retirar comentarios abaixo
        /*
        Map<String, Object> nomeResult = cadastralUtil.findNome(databaseScan.findProdutoTable(C_NOME, requestMap, config.getContextoFetchColumns(C_NOME)));
        if (nomeResult.isEmpty()) {
            writeResponse(exchng.getOut(), Collections.emptyMap(), null, request);
            return;
        }

        final Object first = cadastralUtil.validaSemResultadosGetFirst(nomeResult, CpfCnpjNaoEncontradoException::new);
        Map<String, Object> map = first instanceof Map ? (Map<String, Object>) first : new HashMap<>();

        int menorIdade = isMenorIdade(map.getOrDefault(config.getColumnQualifier().getDataNascimento(), "").toString());
        if ( menorIdade == 0 || menorIdade == 1) {
            //throw new CpfCnpjMenorIdadeException();
            writeResponse(exchng.getOut(), Collections.emptyMap(), null, request);
            return;
        }*/

        Optional<List<Map<String, Object>>> result = databaseScan.findProdutoTableList(contexto, requestMap, config.getContextoFetchColumns(contexto));

        List<Map<String, Object>> limitList = limit > 0 ?
                result.orElseGet(Collections::emptyList).stream().limit(limit).collect(toList())
                : result.orElseGet(Collections::emptyList);

        writeResponse(exchng.getOut(), limitList, contexto, request);
    }

    private void executeBigTableEnderecosList(final Exchange exchng, final String contexto, final RequestData request) {
        executeBigTableEnderecosList(exchng, contexto, request, 0);
    }

    private void executeBigTableEnderecosList(final Exchange exchng, final String contexto, final RequestData request, int limit) {

        final Map<String, String> requestMap = readRequestHeaders(request);
        //quando for implantar validacao de menor de idade no socket, retirar comentarios abaixo
        /*
        Map<String, Object> nomeResult = cadastralUtil.findNome(databaseScan.findProdutoTable(C_NOME, requestMap, config.getContextoFetchColumns(C_NOME)));
        if (nomeResult.isEmpty()) {
            writeResponse(exchng.getOut(), Collections.emptyMap(), null, request);
            return;
        }

        final Object first = cadastralUtil.validaSemResultadosGetFirst(nomeResult, CpfCnpjNaoEncontradoException::new);
        Map<String, Object> map = first instanceof Map ? (Map<String, Object>) first : new HashMap<>();

        int menorIdade = isMenorIdade(map.getOrDefault(config.getColumnQualifier().getDataNascimento(), "").toString());
        if ( menorIdade == 0 || menorIdade == 1) {
            //throw new CpfCnpjMenorIdadeException();
            writeResponse(exchng.getOut(), Collections.emptyMap(), null, request);
            return;
        }*/

        Optional<List<Map<String, Object>>> result = databaseScan.findProdutoTableList(contexto, requestMap, config.getContextoFetchColumns(contexto));
        List<Map<String, Object>> limitList = limit > 0 ?
                result.orElseGet(Collections::emptyList).stream().limit(limit).collect(toList())
                : result.orElseGet(Collections::emptyList);
        cadastralUtil.findEndereco(limitList);
        writeResponse(exchng.getOut(), limitList, contexto, request);
    }

    private void executeBigTableList(final Exchange exchng, final String contexto, final RequestData request) {
        executeBigTableList(exchng, contexto, request, 0);
    }

    private void executeBigTableDocumentoTipoList(final Exchange exchng, final String contexto, final RequestData request, int limit) {
        final Map<String, String> requestMap = readRequestHeaders(request);

        //quando for implantar validacao de menor de idade no socket, retirar comentarios abaixo
        /*
        Map<String, Object> nomeResult = cadastralUtil.findNome(databaseScan.findProdutoTable(C_NOME, requestMap, config.getContextoFetchColumns(C_NOME)));
        if (nomeResult.isEmpty()) {
            writeResponse(exchng.getOut(), Collections.emptyMap(), null, request);
            return;
        }

        final Object first = cadastralUtil.validaSemResultadosGetFirst(nomeResult, CpfCnpjNaoEncontradoException::new);
        Map<String, Object> map = first instanceof Map ? (Map<String, Object>) first : new HashMap<>();

        int menorIdade = isMenorIdade(map.getOrDefault(config.getColumnQualifier().getDataNascimento(), "").toString());
        if ( menorIdade == 0 || menorIdade == 1) {
            //throw new CpfCnpjMenorIdadeException();
            writeResponse(exchng.getOut(), Collections.emptyMap(), null, request);
            return;
        }*/

        Optional<List<Map<String, Object>>> result = databaseScan.findProdutoTableList(contexto, requestMap, config.getContextoFetchColumns(contexto));

        requestMap.put("tipo", "RG");
        Optional<List<Map<String, Object>>> documentoList = cadastralUtil.aplicaFiltroDocumento(requestMap, result);

        List<Map<String, Object>> limitList = limit > 0 ?
                documentoList.orElse(new ArrayList<Map<String, Object>>()).stream().limit(limit).collect(toList()) :
                documentoList.orElse(new ArrayList<Map<String, Object>>());

        writeResponse(exchng.getOut(), limitList, contexto, request);
    }

    private void executeBigTableDocumentoTipoList(final Exchange exchng, final String contexto, final RequestData request) {
        executeBigTableDocumentoTipoList(exchng, contexto, request, 0);
    }

    private void executeBigTableTelefonesList(final Exchange exchng, final String contexto, final RequestData request, final int limit) {
        final Map<String, String> requestMap = readRequestHeaders(request);

        //quando for implantar validacao de menor de idade no socket, retirar comentarios abaixo
        /*
        Map<String, Object> nomeResult = cadastralUtil.findNome(databaseScan.findProdutoTable(C_NOME, requestMap, config.getContextoFetchColumns(C_NOME)));
        if (nomeResult.isEmpty()) {
            writeResponse(exchng.getOut(), Collections.emptyMap(), null, request);
            return;
        }

        final Object first = cadastralUtil.validaSemResultadosGetFirst(nomeResult, CpfCnpjNaoEncontradoException::new);
        Map<String, Object> map = first instanceof Map ? (Map<String, Object>) first : new HashMap<>();

        int menorIdade = isMenorIdade(map.getOrDefault(config.getColumnQualifier().getDataNascimento(), "").toString());
        if ( menorIdade == 0 || menorIdade == 1) {
            //throw new CpfCnpjMenorIdadeException();
            writeResponse(exchng.getOut(), Collections.emptyMap(), null, request);
            return;
        }*/

        Optional<List<Map<String, Object>>> result = databaseScan.findProdutoTableList(contexto, requestMap, config.getContextoFetchColumns(contexto));
        List<Map<String, Object>> contatosList = cadastralUtil.findTelefonesFixosECelulares(result,
                limit == QUANTIDADE_TELEFONE_GOLDEN ? true : false);

        List<Map<String, Object>> limitList = limit > 0 ?
                contatosList.stream().limit(limit).collect(toList())
                : contatosList;

        writeResponse(exchng.getOut(), limitList, contexto, request);
    }

    private void executeBigTableTelefonesList(final Exchange exchng, final String contexto, final RequestData request) {
        executeBigTableTelefonesList(exchng, contexto, request, 0);
    }

    //quando for implantar validacao de menor de idade no socket, retirar comentarios abaixo
    /*
    @CaptureSpan
    private Optional executeBigTableNome(final Exchange exchng, final String contexto){
        final Map<String, String> requestMap = consultaRestServiceBase.readRequestHeaders(exchng.getIn());
        return executeBigTableNome(exchng, contexto,  requestMap);
    }

    @CaptureSpan
    private Optional executeBigTableNome(final Exchange exchng, final String contexto, Map<String, String> requestMap){
        configureMDC(exchng);
        return databaseScan.findProdutoTable(contexto, requestMap,  config.getContextoFetchColumns(contexto));
    }*/

    private void writeResponse(final Message out, final Map<String, Object> responseMap, final String contexto, final RequestData request) {
        ResponseData response = ResponseData.parse(
                (Map<String, Object>) performDeParaCampos(responseMap, contexto, config),
                request.getDocumento(),
                request.getTransacao(),
                socketConfig);

        String mgs = response.toString();
        LOGGER.info("[SOCKET] Mensagem enviada: {}", mgs);
        out.setBody(mgs, String.class);

        final Instant finished = Instant.now();
        LOGGER.info("cpf={} statuscode={} started={} finished={} elapsedtime={} ip={} service={}",
                md5Hash(request.getDocumento()), 200, getDateFormatted(request.getStarted()), getDateFormatted(finished),
                Duration.between(request.getStarted(), finished).toMillis(),
                request.getIpExterno(), request.getTransacao());
    }

    private void writeResponse(final Message out, final List<Map<String, Object>> responseList, final String contexto, final RequestData request) {
        ResponseData response = ResponseData.parse(
                responseList.stream().map(m -> (Map<String, Object>) performDeParaCampos(m, contexto, config)).collect(toList()),
                request.getDocumento(),
                request.getTransacao(),
                socketConfig);

        String mgs = response.toString();
        LOGGER.info("[SOCKET] Mensagem enviada: {}", mgs);
        out.setBody(mgs, String.class);

        final Instant finished = Instant.now();
        LOGGER.info("cpf={} statuscode={} started={} finished={} elapsedtime={} ip={} service={}",
                md5Hash(request.getDocumento()), 200, getDateFormatted(request.getStarted()), getDateFormatted(finished),
                Duration.between(request.getStarted(), finished).toMillis(),
                request.getIpExterno(), request.getTransacao());
    }

    private Map<String, String> readRequestHeaders(final RequestData request) {
        return new HashMap<String, String>() {{
            put("cpf", request.getDocumento());
        }};
    }
}
