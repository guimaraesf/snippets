package br.com.bs;

import io.opencensus.common.Scope;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;


/**
 *
 * @author BlueShift
 */

@Configuration
@SpringBootApplication(scanBasePackages = "br.com.bs")
public class ApiBaseCadastral {
    private static final Tracer tracer = Tracing.getTracer();

    public static void main(String[] args) {

        try (Scope ss = tracer.spanBuilder("ApiBaseCadastral").setSampler(Samplers.alwaysSample()).startScopedSpan()) {
            SpringApplication.run(ApiBaseCadastral.class, args);
        }

    }
}

//import javax.annotation.PostConstruct;
//
//@Configuration
//@SpringBootApplication(scanBasePackages = "br.com.bs")
//public class ApiBaseCadastral {
//    private static final Tracer tracer = Tracing.getTracer();
//    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
//
//    @Autowired
//    Environment env;
//
//    @CaptureTransaction
//    public static void main(String[] args) {
//        try (Scope ss = tracer.spanBuilder("ApiBaseCadastral").setSampler(Samplers.alwaysSample()).startScopedSpan()) {
//            SpringApplication.run(ApiBaseCadastral.class, args);
//        }
//
//
//    }
//
//    @PostConstruct
//    public void postConstruct() {
//        if (env.getProperty("app.bigtable.projectId").equals("data-3660")) ElasticApmAttacher.attach();
//    }
//}