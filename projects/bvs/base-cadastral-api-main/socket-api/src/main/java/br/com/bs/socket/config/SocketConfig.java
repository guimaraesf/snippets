package br.com.bs.socket.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Profile("socket")
@Configuration
@ConfigurationProperties("app.socket")
public class SocketConfig {

    private Integer port;
    private Integer maxIdlePoolSize;
    private String minIdleEvictable;
    private String byeCommand;
    private Map<String, List<Registro>> layoutServico = new HashMap<>();

    public String getServiceEndFile() {
        return "\r";
    }

    public Integer getPort() {
        return port;
    }

    public Integer getMaxIdlePoolSize() {
        return maxIdlePoolSize;
    }

    public void setMaxIdlePoolSize(Integer maxIdlePoolSize) {
        this.maxIdlePoolSize = maxIdlePoolSize;
    }

    public String getMinIdleEvictable() {
        return minIdleEvictable;
    }

    public void setMinIdleEvictable(String minIdleEvictable) {
        this.minIdleEvictable = minIdleEvictable;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getByeCommand() {
        return byeCommand;
    }

    public void setByeCommand(String byeCommand) {
        this.byeCommand = byeCommand;
    }

    public Map<String, List<Registro>> getLayoutServico() {
        return layoutServico;
    }

    public void setLayoutServico(Map<String, List<Registro>> layoutServico) {
        this.layoutServico = layoutServico;
    }

    public enum Pad{
        LEFT,
        RIGHT
    }
    public static class Registro {
        private String campo;
        private Integer tamanho;
        private Pad pad = Pad.RIGHT;

        public String getCampo() {
            return campo;
        }

        public void setCampo(String campo) {
            this.campo = campo;
        }

        public Integer getTamanho() {
            return tamanho;
        }

        public void setTamanho(Integer tamanho) {
            this.tamanho = tamanho;
        }

        public Pad getPad() {
            return pad;
        }

        public void setPad(Pad pad) {
            this.pad = pad;
        }
    }
}
