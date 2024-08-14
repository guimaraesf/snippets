package br.com.bs.rest.config;

import br.com.bs.config.AppConfig;
import br.com.bs.jwt.util.JwtUtil;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.io.*;
import java.nio.charset.StandardCharsets;


@EnableWebSecurity
@EnableSwagger2
@Configuration
public class SwaggerConfig {

    public static String redirectPath;

    @Value("${app.swagger.redirect-path}")
    public void setRedirectPath(String redirectPath) {
        this.redirectPath = redirectPath;
    }

    private static ClassPathResource index;
    private static String indexString;
    private static String cssString;
    private static String bundleString;
    private static ClassPathResource css;
    private static ClassPathResource bundle;
    private static ClassPathResource preset;

    public static JwtUtil jwtUtil = new JwtUtil();

    static {
        String basePath = "/META-INF/resources/webjars/swagger-ui/3.23.8/";
        index = new ClassPathResource(basePath + "index.html");
        css = new ClassPathResource(basePath + "swagger-ui.css");
        bundle = new ClassPathResource(basePath + "swagger-ui-bundle.js");
        preset = new ClassPathResource(basePath + "swagger-ui-standalone-preset.js");
    }


    public static String addTokenHeader(String redirect, String userName, String product){
        // TODO Melhoria: Incluir token no código Java (remover do "replaceAll")
        String redirectWithToken = redirect.replaceAll("replace.token.jwt", jwtUtil.generateToken(userName, product));
        return redirectWithToken;
    }

    public static void configureSwaggerUI(final RouteBuilder restServerBuilder, AppConfig config) throws IOException {

        File oauth2Redirect = new File(redirectPath);
        FileInputStream oauth2RedirectStream = new FileInputStream(oauth2Redirect);
        byte[] data = new byte[(int) oauth2Redirect.length()];
        oauth2RedirectStream.read(data);
        oauth2RedirectStream.close();

        String strOauth2Redirect = new String(data, "UTF-8");


        restServerBuilder.rest("/doc").apiDocs(false)
                .get("/")
                    .to("direct://get/swagger/ui/index")
                .get("/swagger-ui.css")
                    .to("direct://get/swagger/ui/resource/css")
                .get("/swagger-ui-bundle.js")
                    .to("direct://get/swagger/ui/resource/bundle")
                .get("/swagger-ui-standalone-preset.js")
                    .to("direct://get/swagger/ui/resource/preset")
                .get("/oauth2-redirect.html")
                    .to("direct://get/swagger/ui/resource/redirect");


        restServerBuilder.from("direct://get/swagger/ui/index").streamCaching()
                .process((exchange) -> {
                    if(StringUtils.isEmpty(indexString)){
                        String text = StreamUtils.copyToString(index.getInputStream(), StandardCharsets.UTF_8);

                        final AppConfig.Rest restConfig = config.getRest();
                        final Integer docPort = restConfig.getDocPort();

                        indexString = text.replaceAll(
                                "https://petstore.swagger.io/v2/swagger.json",
                                String.format("%s://%s%s%s%s", restConfig.getDocProtocol(),
                                                               restConfig.getDocHost(),
                                                              (docPort != null && docPort > 0) ? ":" + docPort : "",
                                                               config.getContextPath(),
                                                               restConfig.getDocJson())
                        ).replaceAll("swagger-ui.css", "doc/swagger-ui.css")
                                .replaceAll("swagger-ui-bundle.js", "doc/swagger-ui-bundle.js")
                                .replaceAll("swagger-ui-standalone-preset.js", "doc/swagger-ui-standalone-preset.js")
                                // TODO Melhoria: Incluir o parâmetro "clientId" no código Java (remover do "replaceAll")
                                .replaceAll("window.ui = ui", "window.ui = ui \n" +
                                        "  ui.initOAuth({\n" +
                                        "    clientId: \"oauth-client\",\n" +
                                        "  })");
                    }
                    exchange.getOut().setHeader("Content-Type", "text/html");
                    exchange.getOut().setBody(indexString);
                }).routeId("SwaggerUI");
        
        restServerBuilder.from("direct://get/swagger/ui/resource/css").streamCaching()
                .process((exchange) -> {
                    if(StringUtils.isEmpty(cssString)){
                        String text = StreamUtils.copyToString(css.getInputStream(), StandardCharsets.UTF_8);

                        cssString = text.replaceAll(
                                ".curl\\{font-size:12px;min-height:100px;margin:0;padding:10px;resize:none;border-radius:4px;background:#41444e;font-family:monospace;font-weight:600;color:#fff}",
                                ".curl{display:none}");
                    }

                    exchange.getOut().setHeader("Content-Type", "text/css");
                    exchange.getOut().setBody(cssString);
                }).routeId("SwaggerUI_css");

        restServerBuilder.from("direct://get/swagger/ui/resource/bundle").streamCaching()
                .process((exchange) -> {
                    exchange.getOut().setHeader("Content-Type", "text/javascript");

                    bundleString = StreamUtils.copyToString(bundle.getInputStream(), StandardCharsets.UTF_8);
                    bundleString = bundleString.replaceAll("/oauth2-redirect.html", "/api/doc/oauth2-redirect.html");
                    bundleString = bundleString.replaceAll(",S.a.createElement\\(\"h4\",null,\"Curl\"\\),", ",");

                    exchange.getOut().setBody(bundleString);
                }).routeId("SwaggerUI_bundle");

        restServerBuilder.from("direct://get/swagger/ui/resource/preset").streamCaching()
                .process((exchange) -> {
                    exchange.getOut().setHeader("Content-Type", "text/javascript");
                    exchange.getOut().setBody(preset.getInputStream());
                }).routeId("SwaggerUI_preset");

        restServerBuilder.from("direct://get/swagger/ui/resource/redirect").streamCaching()
                .process((exchange) -> {
                    exchange.getOut().setHeader("Content-Type", "text/html");
                    // TODO Melhoria: Adicionar usuário de login no username (para gerar o token com o nome do usuario logado)
                    exchange.getOut().setBody(addTokenHeader(strOauth2Redirect, "Swagger User", "Swagger UI"));
                }).routeId("SwaggerUI_redirect_oauth2");
    }
}
