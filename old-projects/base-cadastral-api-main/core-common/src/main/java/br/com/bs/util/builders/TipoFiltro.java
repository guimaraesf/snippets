package br.com.bs.util.builders;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static br.com.bs.util.builders.TipoFiltro.TipoParametro.*;

public enum TipoFiltro{
    BUSCA_COMPLETA(1, "busca_completa", QUERY),
    CONTEXTO(1, "/contexto/{contexto}", PATH),

    CPF(2, "/cpf/{cpf}", PATH),
    ORIGEM(3, "/origem/{origem}", PATH),
    TIPO(4, "/tipo/{tipo}", PATH),
    VERBO(5, "/verbo/{verbo}", PATH),
    DESTINO(6, "/destino/{destino}", PATH),
    TELEFONE(7, "/telefone/{telefone}", PATH),
    NOME(8, "/nome/{nome}", PATH),
    CPF_TITULAR(9, "/cpf_titular/{cpf_titular}", PATH),
    CPF_RELACIONADO(10, "/cpf_relacionado/{cpf_relacionado}", PATH),
    CEP(2, "/cep/{cep}", PATH);

    Integer orderParam;
    String urlParam;
    TipoParametro tipoParametro;

    TipoFiltro(Integer orderParam, String urlParam, TipoParametro tipoParametro) {
        this.orderParam = orderParam;
        this.urlParam = urlParam;
        this.tipoParametro = tipoParametro;
    }

    public static String paramFiltro(TipoFiltro filtro) {
        return paramsFiltros(filtro).stream().findFirst().orElseThrow(UnsupportedOperationException::new);
    }

    public static Set<String> paramsFiltros() {
        return paramsFiltros(TipoFiltro.values());
    }

    private static Set<String> paramsFiltros(TipoFiltro...filtros) {
        final Pattern p = Pattern.compile("\\{(.*?)\\}");
        return Arrays.stream(filtros)
                .map(f -> f.urlParam)
                .map(p::matcher)
                .map(url -> {
                    final List<String> params = new ArrayList<>();
                    while (url.find())
                        params.add(url.group(1));
                    return params;
                })
                .flatMap(Collection::stream)
                .distinct()
                .collect(Collectors.toSet());
    }

    enum TipoParametro {
        QUERY,
        PATH
    }
}
