package br.com.bs.rest.builders;

import br.com.bs.config.AppConfig;
import br.com.bs.rest.process.MetricsProcess;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.metrics.routepolicy.MetricsRoutePolicyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Profile("metrics-camel")
@Component
public class SchedulerBuilder extends RouteBuilder {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private final CamelContext camelContext;
    private final AppConfig config;

    @Autowired
    public SchedulerBuilder(AppConfig config, CamelContext camelContext) {
        this.config = config;
        this.camelContext = camelContext;
    }

    @Override
    public void configure() throws Exception {

        camelContext.addRoutePolicyFactory(new MetricsRoutePolicyFactory());

        from("quartz2://scheduler/pooling?cron=0+*+*+*+*+?")
                .process(MetricsProcess.getInstance());
    }
}
