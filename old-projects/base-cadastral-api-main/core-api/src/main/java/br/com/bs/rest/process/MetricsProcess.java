package br.com.bs.rest.process;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.metrics.routepolicy.MetricsRegistryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsProcess implements Processor {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private static MetricsProcess metricsProcess;

    public static MetricsProcess getInstance() {
        if(metricsProcess == null)
            metricsProcess = new MetricsProcess();
        return metricsProcess;
    }

    @Override
    public void process(Exchange e) {
        MetricsRegistryService regSvc = e.getContext().hasService(MetricsRegistryService.class);
        regSvc.setPrettyPrint(false);
        LOGGER.info(regSvc.dumpStatisticsAsJson());
    }
}
