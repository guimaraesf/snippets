package br.com.bs.util;

import br.com.bs.config.AppConfig;
import br.com.bs.exception.CpfCnpjAbstractException;
import br.com.bs.exception.CpfCnpjMenorIdadeException;
import br.com.bs.exception.CpfCnpjNaoEncontradoException;
import br.com.bs.exception.CpfCnpjSemResultadoException;
import br.com.bs.interfaces.DatabaseScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static br.com.bs.config.AppConfig.TipoRelacionamento.CONJUGE;
import static br.com.bs.config.AppConfig.TipoRelacionamento.MAE;
import static br.com.bs.config.AppConfig.TipoRelacionamento.PAI;
import static br.com.bs.config.AppConfig.TipoRelacionamento.TR_MAIORES_IDADE;
import static br.com.bs.config.AppConfig.TipoTelefone.CELULAR;
import static br.com.bs.config.AppConfig.TipoTelefone.FIXO;
import static br.com.bs.config.AppConfig.TipoTelefone.NEXTEL;
import static br.com.bs.config.Constants.*;
import static br.com.bs.util.Util.isMenorIdade;
import static br.com.bs.util.Util.performDeParaCampos;
import static java.util.stream.Collectors.toList;
import static org.springframework.util.StringUtils.isEmpty;

@Component
public class CadastralUtil {
    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
    private final AppConfig config;
    private final DatabaseScan databaseScan;

    public static final String MENSAGEM_MENOR_IDADE = "CPF de Menor de idade, não é passível consultar outros dados !";
    public static final String MENSAGEM_MENOR_IDADE_ACIMA_16_ANOS = "CPF de Menor de Idade - entre 16 e 17 anos";

    @Autowired
    public CadastralUtil(AppConfig config, DatabaseScan databaseScan) {

        this.config = config;
        this.databaseScan = databaseScan;
    }

    public Map checkDefaultSituacaoReceita(final String contexto, final Optional resultBusca) {
        final Map<String, Object> resultMap = (Map<String, Object>) resultBusca.orElseGet(() -> new HashMap());

        if (!C_PROTOCOLO.equals(contexto))
            return resultMap;

        String column = config.getColumnQualifier().getSituacaoReceita();
        if (resultMap.isEmpty() || !resultMap.containsKey(column))
            resultMap.put(column, config.getSituacaoReceitaDefault());

        return resultMap;
    }

    public Map<String, Object> findPessoaRelacionada(Optional resultRelacionado) throws NullPointerException {
        List<Map<String, Object>> listRelacionado = (List) resultRelacionado.orElse(new ArrayList<Map>());
        final Map<String, Object> mapRelacionado = new HashMap<>();
        AppConfig.ColumnQualifier columnQualifier = config.getColumnQualifier();
        try{
        listRelacionado.forEach(p -> {
            String tp = p.get(columnQualifier.getTipoRelacionamento()).toString();
            Object nome = p.get(columnQualifier.getNomeRelacionada());
            Object metrica = p.get(columnQualifier.getMetricaPessoaRelacionada());
            if (MAE.getDescricao().equals(tp) && !mapRelacionado.containsKey(columnQualifier.getMaeServico())) {
                mapRelacionado.put(columnQualifier.getMaeServico(), nome);
                mapRelacionado.put(columnQualifier.getMetricaMaeServico(), metrica);
            } else if (PAI.getDescricao().equals(tp) && !mapRelacionado.containsKey(columnQualifier.getPaiServico())) {
                mapRelacionado.put(columnQualifier.getPaiServico(), nome);
                mapRelacionado.put(columnQualifier.getMetricaPaiServico(), metrica);
            } else if (CONJUGE.getDescricao().equals(tp) && !mapRelacionado.containsKey(columnQualifier.getConjugeServico())) {
                mapRelacionado.put(columnQualifier.getConjugeServico(), nome);
                mapRelacionado.put(columnQualifier.getMetricaConjugeServico(), metrica);
            }
        });
    } catch (NullPointerException e){
            LOGGER.error("Falha ao encontrar pessoa relacionada {}", e);
        }
        return mapRelacionado;
    }

    public Map<String, Object> findNome(Optional resultNome) throws NullPointerException {
        Map<String, Object> mapNome = (Map)resultNome.orElseGet(Collections::emptyMap);

        AppConfig.ColumnQualifier columnQualifier = config.getColumnQualifier();
        if(containsKeyAndNotEmpty(mapNome, columnQualifier.getNomeSocial()))
            mapNome.put(columnQualifier.getNomeServico(), mapNome.get(columnQualifier.getNomeSocial()));
        else if(containsKeyAndNotEmpty(mapNome, columnQualifier.getNomeCivil()))
            mapNome.put(columnQualifier.getNomeServico(), mapNome.get(columnQualifier.getNomeCivil()));

        mapNome.remove(columnQualifier.getNomeSocial());
        mapNome.remove(columnQualifier.getNomeCivil());

        aplicaRegraMatricaParaDtNasc(mapNome);

        mapNome = (Map<String, Object>)performDeParaCampos(mapNome, C_NOME, config);
        return mapNome;
    }

    public List findTelefonesFixosECelulares(Optional result, boolean isMelhor) throws NullPointerException{
        List<Map<String, Object>> contatosList = (List<Map<String, Object>>)result.orElse(new ArrayList<Map>());
        if(!isMelhor)
            return aplicaRegraDDDTelefone_e_replace(contatosList);

        AppConfig.ColumnQualifier columnQualifier = config.getColumnQualifier();
        long countFixo = 0;
        try{
        countFixo = contatosList.stream()
                .filter(i -> i.get(columnQualifier.getTelefoneTipo()).equals(FIXO.getCodigo()))
                .count();
        } catch(NullPointerException e){
            LOGGER.error("Falha ao fazer depara de telefone fixo {}", e);
        }
        long countCelular = 0;
        try {
            countCelular = contatosList.stream()
                    .filter(i -> i.get(columnQualifier.getTelefoneTipo()).equals(CELULAR.getCodigo())
                            || i.get(columnQualifier.getTelefoneTipo()).equals(NEXTEL.getCodigo()))
                    .count();
        } catch (NullPointerException e){
            LOGGER.error("Falha ao fazer depara de celular {}", e);
        }
        if((countCelular == 0 && countFixo > 0)
                || (countFixo == 0 && countCelular > 0)){
            return aplicaRegraDDDTelefone_e_replace(contatosList.stream().limit(3).collect(toList()));
        }else {
            List<Map<String, Object>> fixoList = new ArrayList<>();
            try {
                fixoList = contatosList.stream()
                        .filter(i -> i.get(columnQualifier.getTelefoneTipo()).equals(FIXO.getCodigo()))
                        .limit(2).collect(toList());
            } catch (NullPointerException e) {
                LOGGER.error("Lista de telefones fixo vazia {}", e);
            }
            List<Map<String, Object>> celularList = new ArrayList<>();
            try {
             celularList = contatosList.stream()
                    .filter(i -> i.get(columnQualifier.getTelefoneTipo()).equals(CELULAR.getCodigo())
                            || i.get(columnQualifier.getTelefoneTipo()).equals(NEXTEL.getCodigo()))
                    .limit(fixoList.size() == 2 ? 1 : 2).collect(toList());

            celularList.addAll(fixoList);
            } catch(NullPointerException e){
                LOGGER.error("Lista de celulares vazia {}", e);
            }
            return aplicaRegraDDDTelefone_e_replace(celularList);
        }
    }

    private List aplicaRegraDDDTelefone_e_replace(List<Map<String, Object>> contatosList)  throws NullPointerException{
        AppConfig.ColumnQualifier columnQualifier = config.getColumnQualifier();

        //Concatena DDD
        contatosList.forEach( c -> {
            String numero = String.valueOf(c.get(columnQualifier.getTelefoneNumero()));
            String ddd = String.valueOf(c.get(columnQualifier.getTelefoneDDD()));
            c.put(columnQualifier.getTelefoneNumero(), ddd + " " + numero);
        });

        contatosList.forEach( l -> {
            Map<String, Object> replace = new HashMap<>();
            l.forEach((k, v) -> {
                if (columnQualifier.getTelefoneClassificacao().equals(k))
                    replace.put(k, v.toString().replace(columnQualifier.getTelefoneClassificacao() + "_", ""));
            });
            l.putAll(replace);
        });
        return contatosList;
    }

    public Optional aplicaFiltroDocumento(Map<String, String> requestMap, Optional result) {
        AppConfig.ColumnQualifier columnQualifier = config.getColumnQualifier();
        if(requestMap.containsKey("tipo")){
            List<Map<String, Object>> listDocumentos = (List)result.orElse(new ArrayList<Map>());
            String tipo = requestMap.get("tipo");
            return Optional.of(listDocumentos.stream().filter( d -> {
                String tipoDB = String.valueOf(d.get(columnQualifier.getTipoDocumento()));
                return tipo.equalsIgnoreCase(tipoDB);
            }).collect(toList()));
        }else return result;
    }

    private void aplicaRegraMatricaParaDtNasc(Map<String, Object> mapNome){
        AppConfig.ColumnQualifier columnQualifier = config.getColumnQualifier();
        Integer metricaNascimento = Integer.valueOf(mapNome.getOrDefault(columnQualifier.getDataNascimentoMetrica(), 0).toString());
        if(metricaNascimento < 10) mapNome.remove(columnQualifier.getDataNascimento());
    }

    private boolean containsKeyAndNotEmpty(Map<String, Object> map, String key) {
        return !isEmpty(map.getOrDefault(key, ""));
    }

    public void findEndereco(List<Map<String, Object>> enderecos) {
        AppConfig.ColumnQualifier columnQualifier = config.getColumnQualifier();
        enderecos.forEach( l -> {
            Map<String, Object> result = new HashMap<>();
            l.forEach((k, v) -> {
                if (columnQualifier.getTipoEndereco().equals(k)) {
                    String codigo = v.toString().replace(columnQualifier.getTipoEndereco() + "_", "");
                    result.put(k, codigo);
                }
            });
            l.putAll(result);
        });
    }

    private void removeMenoresDeIdade(List<Map<String, Object>> list) throws CpfCnpjAbstractException {
        final String campoDtNasc = config.getColumnQualifier().getDataNascimento();
        final String campoTipoRelacionamento = config.getColumnQualifier().getTipoRelacionamento();

        final List<Map<String, Object>> menores =
                list.parallelStream()
                .filter(p -> {
                    final String tipoRelacionamento = p.getOrDefault(campoTipoRelacionamento, "").toString();
                    final String dtNsc = p.getOrDefault(campoDtNasc, "").toString();

                    return !TR_MAIORES_IDADE.contains(tipoRelacionamento)
                            && (isEmpty(dtNsc) || (isMenorIdade(dtNsc) != 2));
                }).collect(toList());
        list.removeAll(menores);
    }

    public void validaEhMenorDeIdade(Object obj, String contexto) throws CpfCnpjAbstractException {
        Map<String, Object> map = obj instanceof Map ? (Map<String, Object>) obj : new HashMap<>();
        String dtNascimento = map.getOrDefault(config.getColumnQualifier().getDataNascimento(), "").toString();
        if (isMenorIdade(dtNascimento) == 0) {
            if (C_NOME.equals(contexto) || C_PPE.equals(contexto)) {
                map.clear();
                map.put(config.getColumnQualifier().getDataNascimento(), dtNascimento);
                map.put(config.getColumnQualifier().getMensagemMenor(), MENSAGEM_MENOR_IDADE);
            } else throw new CpfCnpjMenorIdadeException();
        } else if (isMenorIdade(dtNascimento) == 1) {
            if (C_ENDERECO.equals(contexto) || C_NOME.equals(contexto) || C_PROTOCOLO.equals(contexto) || C_TELEFONE.equals(contexto) || C_PESSOARELACIONADA.equals(contexto) || C_PPE.equals(contexto))
                map.put(config.getColumnQualifier().getMensagemMenor(), MENSAGEM_MENOR_IDADE_ACIMA_16_ANOS);
            else throw new CpfCnpjMenorIdadeException();
        }
    }

    public Object validaSemResultadosGetFirst(Object obj, Supplier<CpfCnpjAbstractException> e) throws CpfCnpjAbstractException {
        Object first;
        if (obj instanceof List)
            first = ((List<?>) obj).stream().findFirst().orElseThrow(e);
        else if (obj instanceof Set)
            first = ((Set<?>) obj).stream().findFirst().orElseThrow(e);
        else
            first = (Map) obj;

        if (first instanceof Map && ((Map) first).isEmpty())
            throw e.get();
        if (first instanceof String && first.toString().isEmpty())
            throw e.get();

        return first;
    }

    public void validaExceptionsCadastral(final Object obj, final String contexto, final boolean buscaCompleta, final Map<String, String> requestMap) throws CpfCnpjAbstractException {
        if (C_PESSOARELACIONADA.equals(contexto) && !buscaCompleta)
            removeMenoresDeIdade((List<Map<String, Object>>) obj);

        if (C_NOME.equals(contexto)) {
            final Object first = validaSemResultadosGetFirst(obj, CpfCnpjNaoEncontradoException::new);
            if(!buscaCompleta)
                validaEhMenorDeIdade(first, contexto);
        } else if (contexto != null && !contexto.equals("receita_pj") && !contexto.equals("ccm") && !contexto.equals("opcao_mei")) {
            Map<String, Object> resultado = findNome(databaseScan.findProdutoTable(C_NOME, requestMap, config.getContextoFetchColumns(C_NOME)));
            if (resultado.isEmpty()) {
                CpfCnpjNaoEncontradoException cpfCnpjNaoEncontradoException = new CpfCnpjNaoEncontradoException();
                throw cpfCnpjNaoEncontradoException;
            }
            final Object first = validaSemResultadosGetFirst(resultado, CpfCnpjNaoEncontradoException::new);
            if (!buscaCompleta)
                validaEhMenorDeIdade(first, contexto);
            validaSemResultadosGetFirst(obj, CpfCnpjSemResultadoException::new);
        } else {
            validaSemResultadosGetFirst(obj, CpfCnpjSemResultadoException::new);
        }

    }
}

