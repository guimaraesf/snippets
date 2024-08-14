package br.com.bs.socket.data;

import br.com.bs.exception.ApiException;
import br.com.bs.socket.config.SocketConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static br.com.bs.config.Constants.*;
import static java.util.Collections.emptyMap;
import static org.apache.commons.lang3.StringUtils.*;

public class ResponseData {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResponseData.class);

    private String transacao;
    private String documento;
    private String reservado;
    private int tamanhoTexto;
    private String texto;

    private ResponseData() { }

    private ResponseData(String transacao, String documento, String reservado, int tamanhoTexto, String texto) {
        this.transacao = transacao;
        this.documento = documento;
        this.reservado = reservado;
        this.tamanhoTexto = tamanhoTexto;
        this.texto = texto;
    }

    public String getTransacao() {
        return transacao;
    }

    public String getDocumento() {
        return documento;
    }

    public String getReservado() {
        return reservado;
    }

    public int getTamanhoTexto() {
        return tamanhoTexto;
    }

    public String getTexto() {
        return texto;
    }

    public static ResponseData parse(Object retorno, String documento, String transacao, SocketConfig socketConfig){
        if(retorno instanceof List)
            return parseListItem((List<Map<String, Object>>)retorno, documento, transacao, socketConfig);
        else if(retorno instanceof Map)
            return parseItem((Map<String, Object>)retorno, documento, transacao, socketConfig);
        else
            return parseError(documento, transacao, socketConfig);
    }

    private static ResponseData parseError(String documento, String transacao, SocketConfig socketConfig) {
        return makeParse(emptyMap(), documento, transacao, socketConfig, S_RETORNO_ERRO);
    }

    private static ResponseData parseItem(Map<String, Object> responseMap, String documento, String transacao, SocketConfig socketConfig) {
        return makeParse(responseMap, documento, transacao, socketConfig, responseMap.isEmpty() ? S_RETORNO_VAZIO : S_RETORNO_FIM);
    }

    private static ResponseData parseListItem(List<Map<String, Object>> responseList, String documento, String transacao, SocketConfig socketConfig) {
        final StringBuilder registro = new StringBuilder();

        if(responseList.isEmpty())
            registro.append(makeParseRegistro(emptyMap(), transacao, socketConfig, S_RETORNO_VAZIO));
        else
            IntStream.range(0, responseList.size())
                    .forEach(index -> {
                        String retorno = (index != responseList.size() - 1) ? S_RETORNO_MAIS : S_RETORNO_FIM;
                        registro.append(makeParseRegistro(responseList.get(index), transacao, socketConfig, retorno));
                    });
        return makeCabecalhoRegistro(documento, transacao, socketConfig, registro.toString());
    }

    private static String makeParseRegistro(Map<String, Object> responseMap, String transacao, SocketConfig socketConfig, String retorno) {
        try {
            StringBuilder dados = new StringBuilder();

            if(!S_RETORNO_VAZIO.equals(retorno) && !S_RETORNO_ERRO.equals(retorno)){
                socketConfig.getLayoutServico().get(transacao).forEach(i -> {
                    String campo = i.getCampo();
                    String valor = getMapValue(responseMap, campo);
                    valor = "NUM_CPF".equals(campo) ?
                            StringUtils.substring(valor, (valor.length() - i.getTamanho()), valor.length()) :
                            StringUtils.substring(valor, 0, i.getTamanho())
                            ;

                    if(SocketConfig.Pad.LEFT.equals(i.getPad())) {
                        valor = leftPad(valor, i.getTamanho(), "0");

                        if ("COD_STS_RCT".equals(campo)) {
                            valor = parseCodigoSituacaoReceita(valor, i);
                        }
                    }
                    else {
                        valor = rightPad(valor, i.getTamanho(), " ");
                    }

                    dados.append(valor);
                });
            }

            StringBuilder registro = new StringBuilder(rightPad(transacao, 8, " "));
            registro.append(leftPad(S_VERSAO, 2, "0"));
            registro.append(retorno);
            registro.append(dados.toString());

            registro.insert(0, leftPad(String.valueOf(registro.toString().length()), 5, "0"));

            return registro.toString();
        }catch (Exception e){
            String m = "Erro ao realizar parse de resposta executada: ";
            LOGGER.error(m, e);
            throw new ApiException(m + e.getMessage(), e);
        }
    }

    /**
     * when valor = '0000000-3' then valor = '-00000003'
     */
    private static String parseCodigoSituacaoReceita(String valor, SocketConfig.Registro registro) {
        boolean isCodigoNegativo = valor.charAt(registro.getTamanho() - 2) == '-';

        if (isCodigoNegativo) {
            valor = valor.replace("-", "0").replaceFirst("0", "-");
        }

        return valor;
    }

    private static ResponseData makeParse(Map<String, Object> responseMap, String documento, String transacao, SocketConfig socketConfig, String retorno) {
        String registro = makeParseRegistro(responseMap, transacao, socketConfig, retorno);
        return makeCabecalhoRegistro(documento, transacao, socketConfig, registro);
    }

    private static ResponseData makeCabecalhoRegistro(String documento, String transacao, SocketConfig socketConfig, String registro) {
        StringBuilder texto = new StringBuilder(registro);
        int length = texto.length();
        texto.append(socketConfig.getServiceEndFile());
        return new ResponseData(transacao, documento, "", length, texto.toString());
    }

    private static String getMapValue(Map<String, Object> responseMap, String key){
        return responseMap != null && !responseMap.isEmpty() && responseMap.containsKey(key)
                ? responseMap.get(key).toString()
                : "";
    }

    @Override
    public String toString() {
        return  rightPad(getTransacao(), 8, " ") +
                leftPad(S_VERSAO, 2, "0") +
                rightPad(getDocumentoParsed(), 20, " ") +
                leftPad(getReservado(), 20, "0") +
                leftPad(String.valueOf(getTamanhoTexto()), 5, "0") +
                getTexto();
    }

    private String getDocumentoParsed() {
        return "CPF" + substring(getDocumento(), 3);
    }
}
