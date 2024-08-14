package br.com.bs.socket.data;

import br.com.bs.exception.ApiException;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.util.Date;

import static org.apache.commons.lang3.StringUtils.substring;

public class RequestData {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestData.class);

    private Instant started;
    private String ipExterno;

    private String transacao;
    private String versao;
    private String documento;
    private String reservado;
    private String fim;

    private RequestData() { }

    public Instant getStarted() {
        return started;
    }

    public void setStarted(Instant started) {
        this.started = started;
    }

    public String getTransacao() {
        return transacao;
    }

    public void setTransacao(String transacao) {
        this.transacao = transacao;
    }

    public String getVersao() {
        return versao;
    }

    public void setVersao(String versao) {
        this.versao = versao;
    }

    public String getDocumento() {
        return documento;
    }

    public void setDocumento(String documento) {
        this.documento = documento;
    }

    public String getReservado() {
        return reservado;
    }

    public void setReservado(String reservado) {
        this.reservado = reservado;
    }

    public String getFim() {
        return fim;
    }

    public void setFim(String fim) {
        this.fim = fim;
    }

    public String getIpExterno() {
        return ipExterno;
    }

    public void setIpExterno(String ipExterno) {
        this.ipExterno = ipExterno;
    }

    public static RequestData parse(String data, Date started, String ipExterno) throws ApiException{

        try{
            RequestData r = new RequestData();
            r.setStarted(started.toInstant());
            r.setIpExterno(ipExterno);


            int atualPosition = 8;
            r.setTransacao(substring(data, 0, atualPosition).trim());

            atualPosition += 2;
            r.setVersao(substring(data, atualPosition - 2, atualPosition).trim());

            atualPosition += 20;

            String documento = substring(data, atualPosition - 20, atualPosition).trim();
            documento = documento.replace("CPF", "000");
            documento = documento.length() < 14 ? "000" + documento : documento;

            r.setDocumento(documento);

            atualPosition += 20;
            r.setReservado(substring(data, atualPosition - 20, atualPosition).trim());

            atualPosition += 1;
            r.setFim(substring(data, atualPosition - 1, atualPosition).trim());

            return r;
        }catch (Exception e){
            String m = "Formato inválido da mensagem: ";
            LOGGER.error(m, e);
            throw new ApiException(HttpStatus.BAD_REQUEST.value(), m + e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return "RequestData{" +
                "transacao='" + transacao + '\'' +
                ", versao='" + versao + '\'' +
                ", documento='" + documento + '\'' +
                ", reservado='" + reservado + '\'' +
                ", fim='" + fim + '\'' +
                '}';
    }
}
