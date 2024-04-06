package br.com.bs.rest.v1;

import br.com.bs.config.AppConfig;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static br.com.bs.util.builders.RestServerBuilderUtil.createRestFiltro;
import static br.com.bs.util.builders.RestServerBuilderUtil.createRestVersion;
import static br.com.bs.util.builders.TipoFiltro.CPF;
import static br.com.bs.util.builders.TipoFiltro.ORIGEM;
import static br.com.bs.util.builders.TipoFiltro.TIPO;

@Component
public class RestServerBuilderV1 {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    private final AppConfig config;

    @Autowired
    public RestServerBuilderV1(AppConfig config) {
        this.config = config;
    }
    
    public void configureVersionRest(RouteBuilder builder){

        final List msgDefinitions = Collections.emptyList();
        /**
         * V1
         */
        final String version = "v1";
        /** CONTATO */
        final RestDefinition restContato = createRestVersion(builder, version, "contato", "Pesquisas de Contatos");
        restContato.setEnableCORS(false);
        //1) Serviço: SPMD_CONTATO_POR_CPF – Parâmetro: CPF;
        createRestFiltro(this.config, LOGGER, builder,  restContato, "findContatoProdutoTable", version, msgDefinitions, ArrayList.class, CPF);

        //2) Serviço: SPMD_CONTATO_POR_CPF_ORIGEM – Parâmetros: CPF + ORIGEM;
        createRestFiltro(this.config, LOGGER, builder, restContato, "findContatoProdutoTable", version, msgDefinitions, ArrayList.class, CPF, ORIGEM);

        /** DOCUMENTO */
        final RestDefinition restDocumento = createRestVersion(builder,  version,"documento", "Pesquisas de cadastros");
        //4) Serviço: SPMD_DOCUMENTO_POR_CPF – Parâmetro: CPF;
        createRestFiltro(this.config, LOGGER, builder, restDocumento, "findDocumentoProdutoTable", version, msgDefinitions, ArrayList.class, CPF);

        //5) Serviço: SPMD_DOCUMENTO_POR_CPF_ORIGEM – Parâmetros: CPF + ORIGEM;
        createRestFiltro(this.config, LOGGER, builder, restDocumento, "findDocumentoProdutoTable", version, msgDefinitions, ArrayList.class, CPF, ORIGEM);

        //Serviço: SPMD_MELHOR_DOCUMENTO_POR_CPF_TIPO – Parâmetros: CPF + TIPO;
        createRestFiltro(this.config, LOGGER, builder, restDocumento, "findDocumentoProdutoTable", version, msgDefinitions, ArrayList.class, CPF, TIPO);

        /** ENDERECO */
        final RestDefinition restEndereco = createRestVersion(builder,  version, "endereco", "Pesquisas de endereços");
        //7) Serviço: SPMD_ENDERECO_POR_CPF – Parâmetro: CPF;
        createRestFiltro(this.config, LOGGER, builder, restEndereco,  "findEnderecoProdutoTable", version, msgDefinitions, ArrayList.class, CPF);

        //8) Serviço: SPMD_ENDERECO_POR_CPF_ORIGEM – Parâmetros: CPF + ORIGEM;
        createRestFiltro(this.config, LOGGER, builder, restEndereco, "findEnderecoProdutoTable", version, msgDefinitions, ArrayList.class, CPF, ORIGEM);

        /** IDENTIFICACAO */
        final RestDefinition restIdentificacao = createRestVersion(builder,  version,"identificacao", "Pesquisas de identificações");
        //10)Serviço: SPMD_IDENTIFICACAO_POR_CPF – Parâmetro: CPF;
        createRestFiltro(this.config, LOGGER, builder, restIdentificacao, "findIdentificacaoProdutoTable", version, msgDefinitions, HashMap.class, CPF);

        //11)Serviço: SPMD_IDENTIFICACAO_POR_CPF_ORIGEM – Parâmetros: CPF + ORIGEM;
        createRestFiltro(this.config, LOGGER, builder, restIdentificacao, "findIdentificacaoProdutoTable", version, msgDefinitions, HashMap.class, CPF, ORIGEM);

        /** OBITO */
        final RestDefinition restObito = createRestVersion(builder,  version,"obito", "Pesquisas de óbito");

        //SPMD_OBITO_POR_CPF
        createRestFiltro(this.config, LOGGER, builder, restObito, "findObitoProdutoTable", version, msgDefinitions, HashMap.class, CPF);

        /** PPE */
        final RestDefinition restPPE = createRestVersion(builder,  version,"ppe", "Pesquisas de ppe");

        //SPMD_PPE_POR_CPF
        createRestFiltro(this.config, LOGGER, builder, restPPE, "findPPEProdutoTable", version, msgDefinitions, HashMap.class, CPF);

        /** DEPENDENTES */
        final RestDefinition restDependentes = createRestVersion(builder,  version,"dependentes", "Pesquisas de dependentes");
        createRestFiltro(this.config, LOGGER, builder, restDependentes, "findDependenteProdutoTable", version, msgDefinitions, ArrayList.class, CPF);

        /** OCUPACAO */
        final RestDefinition restOcupacao = createRestVersion(builder,  version,"ocupacao", "Pesquisas de Ocupacao");
        createRestFiltro(this.config, LOGGER, builder, restOcupacao, "findOcupacaoProdutoTable", version, msgDefinitions, ArrayList.class, CPF);

        /** ENDERECO DE INSTALACAO */
        final RestDefinition restEndInst = createRestVersion(builder,  version,"end_instalacao", "Pesquisas de Telefones Fixos associados a Enderecos de Instalacao");
        createRestFiltro(this.config, LOGGER, builder, restEndInst, "findEndInstProdutoTable", version, msgDefinitions, ArrayList.class, CPF);
    } 

}

