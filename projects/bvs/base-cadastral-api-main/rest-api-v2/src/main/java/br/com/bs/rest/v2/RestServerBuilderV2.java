package br.com.bs.rest.v2;

import br.com.bs.config.AppConfig;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;

import static br.com.bs.util.builders.RestServerBuilderUtil.*;
import static br.com.bs.util.builders.TipoFiltro.*;

@Component
public class RestServerBuilderV2 {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private final AppConfig config;

    @Autowired
    public RestServerBuilderV2(AppConfig config) {
        this.config = config;
    }


    public void configureVersionRest(RouteBuilder builder) {
        final String version = "v2";

        /* findContextoProdutoTable */
        final RestDefinition restContexto = createRestVersion(builder, version, "", "Pesquisas de Contextos isolados");
        restContexto.setEnableCORS(false);
        createRestFiltro(this.config, LOGGER, builder, restContexto, "findContextoProdutoTable", version, ArrayList.class, CONTEXTO, CPF, BUSCA_COMPLETA);

        /* findReversedIndexTable */
        final RestDefinition restReversedIndex = createRestVersion(builder, version, "grafo", "Pesquisas a partir de filtros dinâmicos em base em modelo Reversed Index");
        createRestFiltro(this.config, LOGGER, builder, restReversedIndex, "findReversedIndexTable", version, GENERIC_REST_RESPONSES, ArrayList.class, ORIGEM, VERBO);
        createRestFiltro(this.config, LOGGER, builder, restReversedIndex, "findReversedIndexTable", version, GENERIC_REST_RESPONSES, ArrayList.class, ORIGEM, VERBO, DESTINO);

        /* scpcFone */
        final RestDefinition enderecoInstalacao = createRestVersion(builder, version, "scpcFone", "Pesquisas a Endereço de Instalação");
        createRestFiltro(this.config, LOGGER, builder, enderecoInstalacao, "findScpcFoneTable", version, GENERIC_REST_RESPONSES, ArrayList.class, TELEFONE);

        /* ppe_pessoa */
        final RestDefinition ppe = createRestVersion(builder, version, "ppe", "Pesquisas a partir de filtros dinâmicos em bases de PPE");
        createRestFiltro(this.config, LOGGER, builder, ppe, "ppe", version, GENERIC_REST_RESPONSES, ArrayList.class, CPF);

        final RestDefinition ppePessoa = createRestVersion(builder, version, "ppePessoa", "Pesquisas a partir de filtros dinâmicos em bases de PPE");
        createRestFiltro(this.config, LOGGER, builder, ppePessoa, "ppe_pessoa", version, GENERIC_REST_RESPONSES, ArrayList.class, CPF);

        final RestDefinition ppe_pessoa_nome = createRestVersion(builder, version, "ppe_pessoa_nome", "Pesquisas a partir de filtros dinâmicos em bases de PPE");
        createRestFiltro(this.config, LOGGER, builder, ppe_pessoa_nome, "ppe_pessoa_nome", version, GENERIC_REST_RESPONSES, ArrayList.class, NOME);

        final RestDefinition ppe_pessoa_mandato = createRestVersion(builder, version, "ppe_pessoa_mandato", "Pesquisas a partir de filtros dinâmicos em bases de PPE");
        createRestFiltro(this.config, LOGGER, builder, ppe_pessoa_mandato, "ppe_pessoa_mandato", version, GENERIC_REST_RESPONSES, ArrayList.class, CPF);

        final RestDefinition ppe_pessoa_relacionados = createRestVersion(builder, version, "ppe_pessoa_relacionados", "Pesquisas a partir de filtros dinâmicos em bases de PPE");
        createRestFiltro(this.config, LOGGER, builder, ppe_pessoa_relacionados, "ppe_pessoa_relacionados", version, GENERIC_REST_RESPONSES, ArrayList.class, CPF);

        final RestDefinition ppe_pessoa_titular = createRestVersion(builder, version, "ppe_pessoa_titular", "Pesquisas a partir de filtros dinâmicos em bases de PPE");
        createRestFiltro(this.config, LOGGER, builder, ppe_pessoa_titular, "ppe_pessoa_titular", version, GENERIC_REST_RESPONSES, ArrayList.class, CPF);

        final RestDefinition ppe_pessoa_titular_relacionado = createRestVersion(builder, version, "ppe_pessoa_titular_relacionado", "Pesquisas a partir de filtros dinâmicos em bases de PPE");
        createRestFiltro(this.config, LOGGER, builder, ppe_pessoa_titular_relacionado, "ppe_pessoa_titular_relacionado", version, GENERIC_REST_RESPONSES, ArrayList.class, CPF_TITULAR, CPF_RELACIONADO);

        /*endereco_cep*/
        final RestDefinition endereco_cep = createRestVersion(builder, version, "enderecoCep", "Pesquisas a partir de filtros dinâmicos em bases de endereço através do CEP");
        createRestFiltro(this.config, LOGGER, builder, endereco_cep, "endereco_cep", version, GENERIC_REST_RESPONSES, ArrayList.class, CEP);

        /* outras grafias */
        final RestDefinition outras_grafias = createRestVersion(builder, version, "outrasGrafias", "Pesquisas a partir de filtros dinâmicos em bases de nome, telefone e endereço através do CPF");
        createRestFiltro(this.config, LOGGER, builder, outras_grafias, "outras_grafias", version, GENERIC_REST_RESPONSES, ArrayList.class, CPF);
    }

}

