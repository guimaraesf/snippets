package br.com.bs.config;

import org.apache.camel.CamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "app")
public class AppConfig {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    @Autowired
    private CamelContext camelContext;

    @Value("${management.server.port}")
    private String managementPort;

    private String pid;
    private String contextPath;
    private String apiTimeout;
    private String pidFile;
    private String statusInibido;
    private boolean enableSwaggerUI;

    private ColumnQualifier columnQualifier = new ColumnQualifier();
    private List<String> contextoCargaDireta = new ArrayList<>();
    private List<String> contextoSemIndex = new ArrayList<>();

    private ReversedIndex reversedIndex = new ReversedIndex();
    private ScpcFone scpcfone = new ScpcFone();
    private Rest rest = new Rest();
    private Map<String, Contexto> contextoList = new HashMap<String, Contexto>();

    private String situacaoReceitaDefault;

    public enum TipoRelacionamento {
        MAE("Mae"),
        PAI("Pai"),
        CONJUGE("Conjuge");
        private String descricao;
        TipoRelacionamento(String descricao) {
            this.descricao = descricao;
        }
        public String getDescricao() {
            return descricao;
        }

        public static List<String> TR_MAIORES_IDADE = Arrays.asList(MAE.getDescricao(), PAI.getDescricao(), CONJUGE.getDescricao());
    }
    public enum TipoTelefone {
        FIXO("1"),
        CELULAR("2"),
        NEXTEL("3");
        private String codigo;
        TipoTelefone(String codigo) {
            this.codigo = codigo;
        }
        public String getCodigo() {
            return codigo;
        }
    }

    public static class ColumnQualifier {
        private String situacaoReceita;
        private String tipoEndereco;
        private String telefoneTipo;
        private String dataNascimento;
        private String dataNascimentoMetrica;
        private String nomeServico;
        private String nomeCivil;
        private String nomeSocial;
        private String nomeRelacionada;
        private String tipoRelacionamento;
        private String cpfRelacionado;
        private String metricaPessoaRelacionada;
        private String maeServico;
        private String metricaMaeServico;
        private String paiServico;
        private String metricaPaiServico;
        private String conjugeServico;
        private String metricaConjugeServico;
        private String tipoDocumento;
        private String telefoneClassificacao;
        private String statusContexto;
        private String codOrgm;
        private String telefoneNumero;
        private String telefoneDDD;
        private String mensagemMenor;

        public String getSituacaoReceita() {
            return situacaoReceita;
        }

        public void setSituacaoReceita(String situacaoReceita) {
            this.situacaoReceita = situacaoReceita;
        }

        public String getTipoEndereco() {
            return tipoEndereco;
        }

        public void setTipoEndereco(String tipoEndereco) {
            this.tipoEndereco = tipoEndereco;
        }

        public String getTelefoneTipo() {
            return telefoneTipo;
        }

        public void setTelefoneTipo(String telefoneTipo) {
            this.telefoneTipo = telefoneTipo;
        }

        public String getMetricaPessoaRelacionada() {
            return metricaPessoaRelacionada;
        }

        public String getCpfRelacionado() {
            return cpfRelacionado;
        }

        public void setCpfRelacionado(String cpfRelacionado) {
            this.cpfRelacionado = cpfRelacionado;
        }

        public void setMetricaPessoaRelacionada(String metricaPessoaRelacionada) {
            this.metricaPessoaRelacionada = metricaPessoaRelacionada;
        }

        public String getDataNascimento() {
            return dataNascimento;
        }

        public void setDataNascimento(String dataNascimento) {
            this.dataNascimento = dataNascimento;
        }

        public String getDataNascimentoMetrica() {
            return dataNascimentoMetrica;
        }

        public String getMetricaMaeServico() {
            return metricaMaeServico;
        }

        public void setMetricaMaeServico(String metricaMaeServico) {
            this.metricaMaeServico = metricaMaeServico;
        }

        public String getMetricaPaiServico() {
            return metricaPaiServico;
        }

        public void setMetricaPaiServico(String metricaPaiServico) {
            this.metricaPaiServico = metricaPaiServico;
        }

        public String getMetricaConjugeServico() {
            return metricaConjugeServico;
        }

        public void setMetricaConjugeServico(String metricaConjugeServico) {
            this.metricaConjugeServico = metricaConjugeServico;
        }

        public void setDataNascimentoMetrica(String dataNascimentoMetrica) {
            this.dataNascimentoMetrica = dataNascimentoMetrica;
        }

        public String getNomeServico() {
            return nomeServico;
        }

        public void setNomeServico(String nomeServico) {
            this.nomeServico = nomeServico;
        }

        public String getNomeCivil() {
            return nomeCivil;
        }

        public void setNomeCivil(String nomeCivil) {
            this.nomeCivil = nomeCivil;
        }

        public String getNomeSocial() {
            return nomeSocial;
        }

        public void setNomeSocial(String nomeSocial) {
            this.nomeSocial = nomeSocial;
        }

        public String getNomeRelacionada() {
            return nomeRelacionada;
        }

        public void setNomeRelacionada(String nomeRelacionada) {
            this.nomeRelacionada = nomeRelacionada;
        }

        public String getTipoRelacionamento() {
            return tipoRelacionamento;
        }

        public void setTipoRelacionamento(String tipoRelacionamento) {
            this.tipoRelacionamento = tipoRelacionamento;
        }

        public String getMaeServico() {
            return maeServico;
        }

        public void setMaeServico(String maeServico) {
            this.maeServico = maeServico;
        }

        public String getPaiServico() {
            return paiServico;
        }

        public void setPaiServico(String paiServico) {
            this.paiServico = paiServico;
        }

        public String getConjugeServico() {
            return conjugeServico;
        }

        public void setConjugeServico(String conjugeServico) {
            this.conjugeServico = conjugeServico;
        }

        public String getTipoDocumento() {
            return tipoDocumento;
        }

        public void setTipoDocumento(String tipoDocumento) {
            this.tipoDocumento = tipoDocumento;
        }

        public String getTelefoneClassificacao() {
            return telefoneClassificacao;
        }

        public void setTelefoneClassificacao(String telefoneClassificacao) {
            this.telefoneClassificacao = telefoneClassificacao;
        }

        public String getStatusContexto() {
            return statusContexto;
        }

        public void setStatusContexto(String statusContexto) {
            this.statusContexto = statusContexto;
        }

        public String getCodOrgm() {
            return codOrgm;
        }

        public void setCodOrgm(String codOrgm) {
            this.codOrgm = codOrgm;
        }

        public String getTelefoneNumero() {
            return telefoneNumero;
        }

        public void setTelefoneNumero(String telefoneNumero) {
            this.telefoneNumero = telefoneNumero;
        }

        public String getTelefoneDDD() {
            return telefoneDDD;
        }

        public void setTelefoneDDD(String telefoneDDD) {
            this.telefoneDDD = telefoneDDD;
        }

        public String getMensagemMenor() {return mensagemMenor; }

        public void setMensagemMenor(String mensagemMenor) { this.mensagemMenor = mensagemMenor;}

    }
    public static class Rest {
        private Integer port;
        private String docJson;
        private Integer docPort;
        private String docProtocol;
        private String docHost;

        public String getDocProtocol() {
            return docProtocol;
        }

        public void setDocProtocol(String docProtocol) {
            this.docProtocol = docProtocol;
        }

        public String getDocHost() {
            return docHost;
        }

        public void setDocHost(String docHost) {
            this.docHost = docHost;
        }

        public Integer getDocPort() {
            return docPort;
        }

        public void setDocPort(Integer docPort) {
            this.docPort = docPort;
        }

        public Integer getPort() {
            return port;
        }

        public void setPort(Integer port) {
            this.port = port;
        }

        public String getDocJson() {
            return docJson;
        }

        public void setDocJson(String docJson) {
            this.docJson = docJson;
        }
    }
    public static class Contexto {

        private String[] colunas = new String[]{};
        private Map<String, String> deParaServicos = new HashMap<>();
        private Map<String, TabelaDominio> deParaDominio = new HashMap<String, TabelaDominio>();

        public static class TabelaDominio {
            private String campo;
            private Map<String, String> dados = new HashMap<String, String>();

            public String getCampo() {
                return campo;
            }

            public void setCampo(String campo) {
                this.campo = campo;
            }

            public Map<String, String> getDados() {
                return dados;
            }

            public void setDados(Map<String, String> dados) {
                this.dados = dados;
            }
        }

        public Map<String, TabelaDominio> getDeParaDominio() {
            return deParaDominio;
        }

        public void setDeParaDominio(Map<String, TabelaDominio> deParaDominio) {
            this.deParaDominio = deParaDominio;
        }

        public String[] getColunas() {
            return colunas;
        }

        public void setColunas(String[] colunas) {
            this.colunas = colunas;
        }

        public Map<String, String> getDeParaServicos() {
            return deParaServicos;
        }

        public void setDeParaServicos(Map<String, String> deParaServicos) {
            this.deParaServicos = deParaServicos;
        }
    }

    public static class ReversedIndex {
        @NotNull private String tableName;
        private int limitRows;
        private List<String> verbos = new ArrayList<>();

        public int getLimitRows() {
            return limitRows;
        }

        public void setLimitRows(int limitRows) {
            this.limitRows = limitRows;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public List<String> getVerbos() {
            return verbos;
        }

        public void setVerbos(List<String> verbos) {
            this.verbos = verbos;
        }
    }

    public static class ScpcFone {
        @NotNull private String tableName;
        private int limitRows;
        private List<String> verbos = new ArrayList<>();

        public int getLimitRows() {
            return limitRows;
        }

        public void setLimitRows(int limitRows) {
            this.limitRows = limitRows;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public List<String> getVerbos() {
            return verbos;
        }

        public void setVerbos(List<String> verbos) {
            this.verbos = verbos;
        }
    }


    public void setContextoList(Map<String, Contexto> contextoList) {
        this.contextoList = contextoList;
    }
    public Map<String, Contexto> getContextoList() {
        return contextoList;
    }

    public String getContextPath() {
        return contextPath;
    }

    public void setContextPath(String contextPath) {
        this.contextPath = contextPath;
    }

    public Rest getRest() {
        return rest;
    }

    public boolean isEnableSwaggerUI() {
        return enableSwaggerUI;
    }

    public void setEnableSwaggerUI(boolean enableSwaggerUI) {
        this.enableSwaggerUI = enableSwaggerUI;
    }

    public String getApiTimeout() {
        return apiTimeout;
    }

    public List<String> getContextoCargaDireta() {
        return contextoCargaDireta;
    }

    public void setContextoCargaDireta(List<String> contextoCargaDireta) {
        this.contextoCargaDireta = contextoCargaDireta;
    }

    public List<String> getContextoSemIndex() {
        return contextoSemIndex;
    }

    public void setContextoSemIndex(List<String> contextoSemIndex) {
        this.contextoSemIndex = contextoSemIndex;
    }

    public void setApiTimeout(String apiTimeout) {
        this.apiTimeout = apiTimeout;
    }

    public String getManagementPort() {
        return managementPort;
    }

    public ReversedIndex getReversedIndex() {
        return reversedIndex;
    }

    public ScpcFone getScpcFone() {
        return scpcfone;
    }

    public void setManagementPort(String managementPort) {
        this.managementPort = managementPort;
    }

    public String getPidFile() {
        return pidFile;
    }

    public void setPidFile(String pidFile) {
        this.pidFile = pidFile;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public CamelContext getCamelContext() {
        return camelContext;
    }

    public void setCamelContext(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    public ColumnQualifier getColumnQualifier() {
        return columnQualifier;
    }

    public String getSituacaoReceitaDefault() {
        return situacaoReceitaDefault;
    }

    public void setSituacaoReceitaDefault(String situacaoReceitaDefault) {
        this.situacaoReceitaDefault = situacaoReceitaDefault;
    }

    public String getStatusInibido() {
        return statusInibido;
    }

    public void setStatusInibido(String statusInibido) {
        this.statusInibido = statusInibido;
    }

    public String[] getContextoFetchColumns(String contexto){
        Contexto contextoObj = getContexto(contexto);
        if(contextoObj == null) return null;

        return contextoObj.getColunas();
    }

    public Map<String, String> getDeParaServico(String contexto){
        Contexto contextoObj = getContexto(contexto);
        if(contextoObj == null) return null;

        return contextoObj.getDeParaServicos();
    }

    public Map<String, Contexto.TabelaDominio> getDeParaDominio(String contexto){
        Contexto contextoObj = getContexto(contexto);
        if(contextoObj == null) return null;
        return contextoObj.getDeParaDominio();
    }

    public boolean isCargaDireta(String contexto){
        return this.getContextoCargaDireta().contains(contexto);
    }

    public boolean hasIndex(String contexto){
        return !this.getContextoSemIndex().contains(contexto);
    }

    private Contexto getContexto(String contexto){
        return getContextoList().getOrDefault(contexto, null);
    }

    @PostConstruct
    public void onStart() {
        initializePID();

        LOGGER.info("Application PID: " + getPid());
        LOGGER.info("Rest port: " + getRest().getPort());
        LOGGER.info("Rest doc: " + getRest().getDocJson());
        LOGGER.info("Management port: " + getManagementPort());
    }

    @PreDestroy
    public void onShutdown() {
        LOGGER.info("Application is shutting down...");
    }

    private void initializePID() {
        try {
            this.pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            Path path = Paths.get(getPidFile());
            Files.write(path, Arrays.asList(this.pid));
        } catch (IOException | RuntimeException ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }

}
