package br.com.bs.socket.builders;

import br.com.bs.socket.config.SocketConfig;
import br.com.bs.socket.service.ConsultaSocketService;
import br.com.bs.util.Util;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Profile("socket")
@Component
public class SocketServerBuilder extends RouteBuilder {

    private final SocketConfig socketConfig;
    private final Logger LOGGER = LoggerFactory.getLogger(ConsultaSocketService.class);

    public static String allowIP;
    //public static String testIp;

    @Value("${app.socket.allow-ip}")
    public void setAllowIP(String allowIP) {
        this.allowIP = allowIP;
    }

    //@Value("${app.socket.test-ip}")
    //public void setTestIp(String testIp) { this.testIp = testIp; }

    @Autowired
    public SocketServerBuilder(SocketConfig socketConfig) {
        this.socketConfig = socketConfig;
    }

    @Override
    public void configure() throws Exception {

        final List<String> optSockets = Stream.of(
                "sync=true",
                "allowDefaultCodec=false",
                "encoder=#socketStringEncoder",
                "decoder=#socketStringDecoder",
                buildProducerPoolMinEvictableIdle(),
                buildProducerPoolMaxIdle()
        ).filter( i -> !i.isEmpty())
         .collect(Collectors.toList());

        //LOGGER.info("############## TestIp: {}", testIp);

        from(String.format("netty4:tcp://0.0.0.0:%d?%s", socketConfig.getPort(), String.join("&", optSockets)))
                .setProperty("remoteAddress", simple("${in.header.CamelNettyRemoteAddress}", String.class))
                .setProperty("allowIP", simple(allowIP, String.class))
                .to("bean:consultaSocketService?method=findProdutoTable").routeId("SocketCadastral");
    }

    @Bean("socketStringEncoder")
    public MessageToMessageEncoder<?> stringEncoder() {
        return new io.netty.handler.codec.string.StringEncoder();
    }

    @Bean("socketStringDecoder")
    public MessageToMessageDecoder<?> stringDecoder() {
        return new io.netty.handler.codec.string.StringDecoder();
    }

    private String buildProducerPoolMaxIdle(){
        if(socketConfig.getMaxIdlePoolSize() != null && socketConfig.getMaxIdlePoolSize() > 0)
            return "producerPoolMaxIdle=" + socketConfig.getMaxIdlePoolSize();
        return "";
    }

    private String buildProducerPoolMinEvictableIdle(){
        final String time = socketConfig.getMinIdleEvictable();
        if(!StringUtils.hasText(time)) return "";

        String prop = "producerPoolMinEvictableIdle=";
        if("0".equals(time) || "-1".equals(time))
            prop = prop + "-1";
        else
            prop = prop + Util.toMilliconds(time).toString();

        return prop;
    }
}
