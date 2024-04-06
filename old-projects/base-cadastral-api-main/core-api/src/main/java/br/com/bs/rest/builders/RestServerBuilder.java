/*
 * BlueShift intelectual property.
 * Contact the BlueShift team before use this source.
 * http://blueshift.com.br/license
 */
package br.com.bs.rest.builders;

import br.com.bs.jwt.util.JwtUtil;
import br.com.bs.config.AppConfig;
import br.com.bs.ratelimiting.service.RateLimitingLoginService;
import br.com.bs.ratelimiting.service.impl.RateLimitingLoginServiceImpl;
import br.com.bs.rest.config.SwaggerConfig;
import br.com.bs.rest.process.HealthCheckProcess;
import br.com.bs.rest.process.RateLimitingLoginProcess;
import br.com.bs.rest.process.RequestTimeProcess;
import br.com.bs.rest.v1.RestServerBuilderV1;
import br.com.bs.rest.v2.RestServerBuilderV2;
import br.com.bs.util.IPUtils;
import br.com.bs.util.ValidUser;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestBindingMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.redis.RedisHealthIndicator;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.stream.Collectors;

import static br.com.bs.util.builders.RestServerBuilderUtil.ROUTE_LOG;
import static br.com.bs.util.builders.RestServerBuilderUtil.TEXT_PLAIN;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.apache.camel.model.rest.RestParamType.header;

@Component
public class RestServerBuilder extends RouteBuilder {

    private final Environment environment;
    private final AppConfig config;
    private final RestServerBuilderV1 serverBuilderV1;
    private final RestServerBuilderV2 serverBuilderV2;
    private final RateLimitingLoginProcess rateLimitingLoginProcess;
    private final RedisHealthIndicator redisHealthIndicator;

    public static String redirectUrl;

    @Value("${app.swagger.redirect-url}")
    public void setRedirectUrl(String redirectUrl) {
        this.redirectUrl = redirectUrl;
    }


    @Autowired
    public RestServerBuilder(
            AppConfig config,
            Environment environment,
            RestServerBuilderV1 serverBuilderV1,
            RestServerBuilderV2 serverBuilderV2,
            RateLimitingLoginService rateLimitingLoginService,
            RedisConnectionFactory redisConnectionFactory
    ) {
        this.config = config;
        this.environment = environment;
        this.serverBuilderV1 = serverBuilderV1;
        this.serverBuilderV2 = serverBuilderV2;
        this.rateLimitingLoginProcess = new RateLimitingLoginProcess(rateLimitingLoginService);
        this.redisHealthIndicator = new RedisHealthIndicator(redisConnectionFactory);
    }

    @Override
    public void configure() throws Exception {

        String info = Arrays.stream(environment.getActiveProfiles()).collect(Collectors.joining(", "));
        info = info.contains("bigtable") ? info : info+", accumulo";

        final AppConfig.Rest restConfig = config.getRest();
        final Integer docPort = restConfig.getDocPort();
        restConfiguration()
                .component("netty4-http")
                .componentProperty("throwExceptionOnFailure", "false")
                .dataFormatProperty("mustBeJAXBElement", "false")
                .bindingMode(RestBindingMode.off)
                .port(restConfig.getPort())
                .contextPath(config.getContextPath())


                .apiHost(restConfig.getDocHost() + ((docPort != null && docPort > 0) ? ":" + docPort : ""))
                .apiContextPath(restConfig.getDocJson())
                .apiProperty("api.title", "API consumo - Base Cadastral :: " + info)
                .enableCORS(false);

        onException(Exception.class)
                .handled(true)
                .removeHeader("user")
                .removeHeader("password")
                .removeHeader("product")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(400))
                .setHeader(Exchange.CONTENT_TYPE, simple(TEXT_PLAIN))
                .setBody(simple("Invalid request."));


        /**
         * security definitions for all routes
         */
        rest().securityDefinitions()
                .oauth2("security-api")
                    // Caso seja necessário enviar o Client ID direto na URL
                    //.authorizationUrl("http://localhost:8077/api/oauth/dialog?client_id=oauth-client").end()
                    .authorizationUrl(redirectUrl).end()
                .end()
                .consumes("application/json").produces("application/json");

        /**
         * health
         */
        rest("/health").get().produces(APPLICATION_JSON).route().process(HealthCheckProcess.getInstance(this.redisHealthIndicator));

        /**
         * login
         */
        rest("/login").get()
                .param()
                    .name("user")
                    .type(header)
                    .dataType("string")
                .endParam()
                .param()
                    .name("password")
                    .type(header)
                    .dataType("string")
                    .dataFormat("password")
                .endParam()
                .param()
                    .name("product")
                    .type(header)
                    .dataType("string")
                .endParam()
                .produces(APPLICATION_JSON).route()
                .setProperty("clientIP", method(IPUtils.class, "getClientIPFromXForwardedFor(${header.x-forwarded-for})"))
                .setProperty("isBlockedIP", method(RateLimitingLoginServiceImpl.class, "isBlockedIP(${property.clientIP})"))
                .setProperty("validUser", method(ValidUser.class, "validUser(${header.user}, ${header.password}, ${property.isBlockedIP})"))
                .removeHeader("user")
                .removeHeader("password")
                .removeHeader("product")
                .choice()
                    .when(simple("${property.isBlockedIP} == true"))
                        .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(429))
                        .setHeader(Exchange.CONTENT_TYPE, simple(TEXT_PLAIN))
                        .setBody(simple("Too many requests."))
                    .stop()
                    .when(simple("${property.validUser} == true || ${property.validUser} == null"))
                        .setProperty("response", method(JwtUtil.class, "generateToken(${header.user}, ${header.product})"))
                        .setBody(simple("{\"token\":\"${property.response}\"}"))
                    .stop()
                .endChoice()
                .otherwise()
                    .process(this.rateLimitingLoginProcess)
                    .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(401))
                    .setHeader(Exchange.CONTENT_TYPE, simple(TEXT_PLAIN))
                    .setBody(simple("Invalid username or password."));

        serverBuilderV1.configureVersionRest(this);
        serverBuilderV2.configureVersionRest(this);

        from(ROUTE_LOG).process(RequestTimeProcess.getInstance()).routeId("PROCESS_ELAPSED_TIME");

        new SwaggerConfig();
        if(config.isEnableSwaggerUI()) SwaggerConfig.configureSwaggerUI(this, config);
    }
}
