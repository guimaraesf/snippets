package br.com.bs.rest.process;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import static br.com.bs.util.Util.getDateFormatted;
import static br.com.bs.util.Util.md5Hash;

public class RequestTimeProcess implements Processor {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private static RequestTimeProcess requestTimeProcess;

    public static RequestTimeProcess getInstance() {
        if(requestTimeProcess == null)
            requestTimeProcess = new RequestTimeProcess();
        return requestTimeProcess;
    }

    @Override
    public void process(Exchange e) {
        Integer code = e.getProperty("statuscode", 200, Integer.class);
        String cpf = e.getProperty("cpf", String.class);
        final Date started = e.getProperty("startedRoute", Date.class);

        final Instant finished = Instant.now();
        long elapsedtime = Duration.between(started.toInstant(), finished).toMillis();

        LOGGER.info("cpf={} statuscode={} started={} finished={} elapsedtime={} ip={} url={}", md5Hash(cpf),
                code,
                getDateFormatted(e.getCreated().getTime()),
                getDateFormatted(finished),
                elapsedtime,
                e.getProperty("ipExterno", String.class),
                e.getProperty("url", String.class));

    }

}
