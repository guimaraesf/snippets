package br.com.bs.util.process;

import br.com.bs.config.Constants;
import br.com.bs.util.Util;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

import java.util.UUID;

public class GenerateUUID implements Processor{

    private static GenerateUUID instance;

    public static GenerateUUID getInstance() {
        if(instance == null) instance = new GenerateUUID();
        return instance;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String uuid = UUID.randomUUID().toString();
        exchange.setProperty(Constants.TRACER_ID, uuid);
        Util.configureMDC(exchange);
    }

}
