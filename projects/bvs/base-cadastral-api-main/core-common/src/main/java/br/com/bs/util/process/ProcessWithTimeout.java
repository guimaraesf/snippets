package br.com.bs.util.process;

import br.com.bs.util.Util;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ProcessWithTimeout implements Processor {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private String bean;
    private String stopWatchName;
    private int timeoutInMillis; // ex: 2s, 1000

    public ProcessWithTimeout(String bean, String stopWatchName, String timeout) {
        this(bean, timeout);
        this.stopWatchName = stopWatchName;
    }

    public ProcessWithTimeout(String bean, String stopWatchName, int timeoutInMillis) {
        this(bean, timeoutInMillis);
        this.stopWatchName = stopWatchName;
    }

    public ProcessWithTimeout(String bean, String timeout) {
        this(bean, Util.toMilliconds(timeout).intValue());
    }

    public ProcessWithTimeout(String bean, int timeoutInMillis) {
        this.bean = bean;
        this.timeoutInMillis = timeoutInMillis;
    }

    @Override
    public void process(Exchange e) throws Exception {
        StopWatch watch = new StopWatch(stopWatchName);
        watch.start("Inicio chamada: " + bean);
        ProducerTemplate p = e.getFromEndpoint().getCamelContext().createProducerTemplate();
        CompletableFuture<Exchange> future = null;
        watch.stop();

        String msg = "Sucesso:\t";
        try {
            watch.start("Criando Future: " + bean);
            future = p.asyncSend(bean, e);
            watch.stop();
            watch.start("Invocando Future: " + bean);
            e.getIn().setBody(timeoutInMillis > 0 ? future.get(timeoutInMillis, TimeUnit.MILLISECONDS) : future.get());

        } catch (TimeoutException ex) {
            LOGGER.error("Tempo esgotado para solicitacao CORE-COMMON {}", ex);
        } finally {
            p.stop();
            watch.stop();
            watch.start("Finalizando: " + bean);
            if (future != null) future.cancel(true);
            watch.stop();
            LOGGER.info(msg + watch.shortSummary());
        }
    }
}
