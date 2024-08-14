package br.com.bs.util.builders;

import br.com.bs.config.AppConfig;
import br.com.bs.config.Constants;
import br.com.bs.exception.*;
import br.com.bs.jwt.util.JwtUtil;
import br.com.bs.util.process.GenerateUUID;
import br.com.bs.util.process.ProcessWithTimeout;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestDefinition;
import org.apache.camel.model.rest.RestOperationResponseMsgDefinition;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;
import static org.apache.camel.builder.Builder.constant;
import static org.apache.camel.builder.Builder.simple;
import static org.apache.camel.model.dataformat.JsonLibrary.Gson;
import static org.apache.camel.model.rest.RestParamType.path;
import static org.apache.camel.model.rest.RestParamType.query;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.OK;

@SuppressWarnings("rawtypes")
public class RestServerBuilderUtil {

    public static final String APPLICATION_JSON = "application/json";
    public static final String TEXT_PLAIN = "text/plain";
    public static final String ROUTE_LOG = "direct:tolog";

    public static final List<RestOperationResponseMsgDefinition> DEFAULT_REST_RESPONSES = Arrays.asList(
            new RestOperationResponseMsgDefinition().code(OK.value()).message(OK.getReasonPhrase()),
            new RestOperationResponseMsgDefinition().code(INTERNAL_SERVER_ERROR.value()).message(INTERNAL_SERVER_ERROR.getReasonPhrase()),
            new RestOperationResponseMsgDefinition().code(ApiTimeoutException.code)
                    .message(ApiTimeoutException.msg),
            new RestOperationResponseMsgDefinition().code(CpfCnpjSemResultadoException.code)
                    .message(CpfCnpjSemResultadoException.msg + " OU " + CpfCnpjNaoEncontradoException.msg),
            new RestOperationResponseMsgDefinition().code(CpfCnpjMenorIdadeException.code)
                    .message(CpfCnpjMenorIdadeException.msg)
    );

    public static final List<RestOperationResponseMsgDefinition> GENERIC_REST_RESPONSES = Arrays.asList(
            new RestOperationResponseMsgDefinition().code(OK.value()).message(OK.getReasonPhrase()),
            new RestOperationResponseMsgDefinition().code(INTERNAL_SERVER_ERROR.value()).message(INTERNAL_SERVER_ERROR.getReasonPhrase()),
            new RestOperationResponseMsgDefinition().code(ApiTimeoutException.code)
                    .message(ApiTimeoutException.msg),
            new RestOperationResponseMsgDefinition().code(CpfCnpjSemResultadoException.code)
                    .message(CpfCnpjSemResultadoException.msg)
    );

    public static RestDefinition createRestVersion(final RouteBuilder builder, final String version, final String path, final String descricao){
        return builder.rest(String.format("/%s/%s", version, path))
                .description(descricao)
                .produces(APPLICATION_JSON);
    }

    public static void createRestFiltro(final AppConfig config,
                                        final Logger logger,
                                        final RouteBuilder builder,
                                        final RestDefinition restBase,
                                        final String method,
                                        final String version,
                                        final Class clazz,
                                        final TipoFiltro...filtros) {
        createRestFiltro(config, logger, builder, restBase, method, version, DEFAULT_REST_RESPONSES, clazz, filtros);
    }

    public static void

    createRestFiltro(final AppConfig config,
                                        final Logger logger,
                                        final RouteBuilder builder,
                                        final RestDefinition restBase,
                                        final String method,
                                        final String version,
                                        final List<RestOperationResponseMsgDefinition> msgDefinitions,
                                        final Class clazz,
                                        final TipoFiltro...filtros){
        List<TipoFiltro> filtrosList = Arrays.asList(filtros);
        filtrosList.sort(comparing(e -> e.orderParam));

        String urlEndpoint = filtrosList.stream()
                .filter( x -> x.tipoParametro.equals(TipoFiltro.TipoParametro.PATH))
                .map(x -> x.urlParam)
                .collect(joining(""));

        restBase.get(urlEndpoint)
                .outType(clazz);

        if(msgDefinitions != null && !msgDefinitions.isEmpty())
            restBase.responseMessages(msgDefinitions);

        //Filtros dinâmicos
        filtrosList.forEach(f -> {
            switch (f){
                case CONTEXTO:
                    restBase.param()
                            .name("contexto")
                            .type(path)
                            .dataType("string")
                            .description("CONTEXTO para realizar consulta")
                            .allowableValues(config.getContextoList().keySet().stream().sorted().collect(Collectors.toList()))
                            .endParam();
                    break;
                case CPF:
                    restBase.param()
                            .name("cpf")
                            .type(path)
                            .dataType("string")
                            .description("CPF para realizar consulta")
                            .endParam();
                    break;
//                case CPFTIT:
//                    restBase.param()
//                            .name("cpfTitular")
//                            .type(path)
//                            .dataType("string")
//                            .description("CPF do Titular PPE que deverá ser filtrado")
//                            .endParam();
//                    break;
//                case CPFREL:
//                    restBase.param()
//                            .name("cpfRel")
//                            .type(path)
//                            .dataType("string")
//                            .description("CPF do Relacionado de PPE que deverá ser filtrado")
//                            .endParam();
//                    break;
                case ORIGEM:
                    restBase.param()
                            .name("origem")
                            .type(path)
                            .dataType("string")
                            .description("ORIGEM para realizar consulta")
                            .endParam();
                    break;
				case TELEFONE:
                    restBase.param()
                            .name("telefone")
                            .type(path)
                            .dataType("string")
                            .description("TELEFONE para realizar consulta")
                            .endParam();
                    break;
                case TIPO:
                    restBase.param()
                            .name("tipo")
                            .type(path)
                            .dataType("string")
                            .description("TIPO que deverá ser filtrado")
                            .endParam();
                    break;
                case VERBO:
                    restBase.param()
                            .name("verbo")
                            .type(path)
                            .dataType("string")
                            .description("VERBO que deverá ser filtrado")
                            .allowableValues(config.getReversedIndex().getVerbos().stream().sorted().collect(Collectors.toList()))
                            .endParam();
                    break;
                case DESTINO:
                    restBase.param()
                            .name("destino")
                            .type(path)
                            .dataType("string")
                            .description("TIPO que deverá ser filtrado")
                            .endParam();
                    break;
                case BUSCA_COMPLETA:
                    restBase.param()
                            .name("busca_completa")
                            .type(query)
                            .required(false)
                            .dataType("boolean")
                            .description("Identifica se deverá ser realizada busca sem limitação nos resultados e registros inibidos")
                            .endParam();
                    break;
                case NOME:
                    restBase.param()
                            .name("nome")
                            .type(path)
                            .dataType("string")
                            .description("Nome para realizar consulta")
                            .endParam();
                    break;
                case CPF_TITULAR:
                    restBase.param()
                            .name("cpf_titular")
                            .type(path)
                            .dataType("string")
                            .description("CPF Titular para realizar consulta")
                            .endParam();
                    break;
                case CPF_RELACIONADO:
                    restBase.param()
                            .name("cpf_relacionado")
                            .type(path)
                            .dataType("string")
                            .description("CPF Relacionado para realizar consulta")
                            .endParam();
                    break;
                case CEP:
                    restBase.param()
                            .name("cep")
                            .type(path)
                            .dataType("string")
                            .description("CEP para realizar consultas de endereços")
                            .endParam();
                    break;
                default: throw new RuntimeException("Tipo de filtro não configurado, disponíveis:" + Arrays.toString(TipoFiltro.values()));
            }
        });

        String routeId = (restBase.getPath().substring(1) + "." + filtrosList.stream().map(x -> x.toString().toLowerCase()).collect(joining(".")))
                .replaceAll("/", ".");

        restBase.security("security-api").route()
                .setProperty("authorized").method(JwtUtil.class, "validToken(${header.Authorization})")
                .choice()
                    .when(simple("${property.authorized} == true"))
                        .setHeader(Exchange.CONTENT_TYPE, builder.simple(TEXT_PLAIN))
                        .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(200))
                        .setBody(builder.simple("Token is valid."))

                        .routeId(routeId)
                        .process(GenerateUUID.getInstance())
                        .setProperty("cpf", builder.simple("${header.cpf}"))
                        .setProperty("url", builder.simple("${header.CamelHttpUrl}"))
                        .setProperty("ipExterno", builder.simple("${header.CamelNettyRemoteAddress}"))
                        .setProperty("startedRoute", builder.simple("${date:now}"))
                        .process(new ProcessWithTimeout(
                                String.format("bean:consultaRestService%s?method=%s", version.toUpperCase(), method),
                                "ConsultaRest",
                                config.getApiTimeout())
                        )
                        .setHeader(Constants.TRACER_ID).exchangeProperty(Constants.TRACER_ID)
                        .removeHeader("user")
                        .removeHeader("password")
                        .removeHeader("product")
                        .marshal().json(Gson)
                        .inOnly(ROUTE_LOG)
                .endChoice()
                .otherwise()
                    .setHeader(Exchange.CONTENT_TYPE, builder.simple(TEXT_PLAIN))
                    .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(401))
                    .removeHeader("user")
                    .removeHeader("password")
                    .removeHeader("product")
                    .setBody(builder.simple("Unauthorized access. Token expired or invalid."))
                .end()

                .onException(Exception.class)
                .handled(true)
                .process(p -> {
                    final Exception e = p.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                    if (e instanceof CpfCnpjAbstractException)
                        logger.info("Rest handled exception: " + e.getMessage());
                    else
                        logger.error("Rest error: " + e.getMessage(), e);
                })
                .removeHeader("cpf")
                .removeHeader("user")
                .removeHeader("password")
                .removeHeader("product")
                .setHeader(Constants.TRACER_ID).exchangeProperty(Constants.TRACER_ID)
                .setHeader(Exchange.HTTP_RESPONSE_CODE, builder.simple("${exception?.statusCode}"))
                .setProperty("statuscode", builder.simple("${exception?.statusCode}"))
                .setHeader(Exchange.CONTENT_TYPE, builder.simple(APPLICATION_JSON))
                .setBody(builder.simple("{ \"erro_mensagem\": \"Ocorreu um erro durante o processamento.\", \"erro_causa\": \"${exception.message}\" }"))
                .inOnly(ROUTE_LOG)
        .endRest();
    }
}
