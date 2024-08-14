package br.com.bs.util.service;

import br.com.bs.util.builders.TipoFiltro;
import org.apache.camel.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.stream.Collectors;

import static br.com.bs.util.Util.md5Hash;

public abstract class ConsultaRestServiceBase {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    public Map<String, String> readRequestHeaders(final Message in) {
        final Map<String, String> requestMap = TipoFiltro.paramsFiltros()
                                    .stream()
                                    .filter( p -> in.getHeaders().containsKey(p))
                                    .collect(Collectors.toMap(k -> k, v -> in.getHeader(v, String.class)));

        final Map<String, String> requestMapLog = TipoFiltro.paramsFiltros()
                .stream()
                .filter( p -> in.getHeaders().containsKey(p))
                .collect(Collectors.toMap(k -> k, v -> in.getHeader(v, String.class)));

        String cpf = requestMapLog.get("cpf");
        if (cpf != null) {
            requestMapLog.put("cpf",md5Hash(cpf));
        }

        LOGGER.info("Checking with headers: " + requestMapLog);

        return requestMap;
    }

    public boolean headerPresentOrTrue(final String nameHeader, final Message in) {
        String headerValue = in.getHeader(nameHeader, String.class);
        return in.getHeaders().containsKey(nameHeader) &&
                (!StringUtils.hasText(headerValue) || Boolean.TRUE.equals(Boolean.valueOf(headerValue)));
    }
}
