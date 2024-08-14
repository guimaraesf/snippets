package br.com.bs.util;

import br.com.bs.config.AppConfig;
import br.com.bs.config.Constants;
import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.StringUtils;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class Util {

    private final static Logger LOGGER = LoggerFactory.getLogger(Util.class);

    private static final DateTimeFormatter PATTERN_US_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static String md5Hash(final String text) {
        try {
            byte[] md = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes("UTF-8"));
            java.math.BigInteger v = new java.math.BigInteger(1, md);
            return String.format("%032x", v);
        } catch (Exception e) {
            return "";
        }
    }

    public static String getKeyByValue(Map<String, String> map, String value, String defaultValue) {
        return map.entrySet()
                .stream()
                .filter(e -> org.apache.commons.lang3.StringUtils.equalsIgnoreCase(e.getValue(), value))
                .map(Entry::getKey)
                .findFirst()
                .orElse(defaultValue);
    }

    public static Object removeBlankOrNull(Object obj) {
        if (obj instanceof Map) {
            Map<String, Object> map = ((Map<String, Object>) obj);
            return map.entrySet().stream()
                    .filter(m -> !StringUtils.isEmpty(m.getValue()))
                    .collect(toMap(Entry::getKey,
                            Entry::getValue,
                            (p1, p2) -> p2));
        } else if (obj instanceof List) {
            List<?> list = ((List<?>) obj);
            return list.stream()
                    .map(m -> Util.removeBlankOrNull(m))
                    .collect(toList());
        }

        return obj;
    }

    public static Object performDeParaCampos(Object obj, String contexto, AppConfig config) {
        if (StringUtils.isEmpty(contexto) || isEmpty(config.getDeParaServico(contexto))) return obj;
        if (obj instanceof Map) {
            Map<String, Object> map = ((Map<String, Object>) obj);
            map.putAll(performDeParaCodigos(map, contexto, config));
            map.putAll(addCodPssFis(map));

            return map.entrySet().stream()
                    .filter(m -> !StringUtils.isEmpty(m.getValue()))
                    .collect(toMap(entry -> getKeyByValue(config.getDeParaServico(contexto), entry.getKey(), entry.getKey()),
                            Entry::getValue,
                            (p1, p2) -> p2));
        } else if (obj instanceof List) {
            List<Map<String, Object>> list = ((List<Map<String, Object>>) obj);

            final Instant started = Instant.now();

            List<Object> result = list.stream()
                    .map(m -> performDeParaCampos(m, contexto, config))
                    .collect(toList());
            printLogger(LOGGER, "performDeParaCampos", contexto, list.stream().findFirst().map(i -> i.get("NUM_CPF_CNPJ")).orElse("").toString(), started);
            return result;
        }

        return obj;
    }

    public static Map<String, Object> performDeParaCodigos(Map<String, Object> result, String contexto, AppConfig config) {
        Map<String, Object> resultDominio = new HashMap<>();
        Map<String, AppConfig.Contexto.TabelaDominio> deParaDominio = config.getDeParaDominio(contexto);
        deParaDominio.keySet().forEach(campo -> {
            Object value = result.get(campo);
            if (result.containsKey(campo) && StringUtils.hasText(value.toString())) {
                AppConfig.Contexto.TabelaDominio dominio = deParaDominio.getOrDefault(campo, null);
                resultDominio.put(dominio.getCampo(), dominio.getDados().get(value.toString().toLowerCase()));
            }
        });
        return resultDominio;
    }

    public static Map<String, Object> addCodPssFis(Map<String, Object> map) {
        final Map<String, Object> result = new HashMap<>();

        String cpf = map.getOrDefault("NUM_CPF_CNPJ", "").toString();
        String origem = map.getOrDefault("COD_ORGM", "").toString();

        if (!map.isEmpty() &&
                StringUtils.hasText(cpf) &&
                StringUtils.hasText(origem)) {
            result.put("COD_PSS_FIS", cpf + origem);
        }
        return result;
    }

    public static String getDateFormatted(Date date) {
        return getDateFormatted(date.toInstant());
    }

    public static String getDateFormatted(long time) {
        return getDateFormatted(Instant.ofEpochMilli(time));
    }

    public static void printLogger(Logger logger, String logID, String contexto, String cpf, Instant started) {
        if (logger.isDebugEnabled()) {
            final Instant finished = Instant.now();
            logger.info("{} :: contexto={} cpf={} started={} finished={} elapsedtime={}",
                    logID, contexto, cpf,
                    getDateFormatted(started), getDateFormatted(finished), Duration.between(started, finished).toMillis());
        }
    }

    public static String getDateFormatted(Instant instant) {
        final DateTimeFormatter formatter = DateTimeFormatter
                .ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .withLocale(Locale.ENGLISH)
                .withZone(ZoneId.of("UTC"));

        return formatter.format(instant);
    }

    private static boolean isEmpty(Map map) {
        return map == null || map.isEmpty();
    }

    public static Long toSeconds(String time) {
        if (!StringUtils.hasText(time)) return 0L;
        long millis = stringTimeToMilliseconds(time);
        return TimeUnit.SECONDS.convert(millis, TimeUnit.MILLISECONDS);
    }

    public static Long toMilliconds(String time) {
        if (!StringUtils.hasText(time)) return 0L;
        return stringTimeToMilliseconds(time);
    }

    private static Long stringTimeToMilliseconds(String time) {
        final long number = time.length() > 1 ? Long.valueOf(time.substring(0, time.length() - 1)) : Long.valueOf(time);
        final String type = time.substring(time.length() - 1).toLowerCase();
        switch (type) {
            case "s":
                return Math.max(1, TimeUnit.SECONDS.toMillis(number));
            case "m":
                return Math.max(1, TimeUnit.MINUTES.toMillis(number));
            case "h":
                return Math.max(1, TimeUnit.HOURS.toMillis(number));
            case "d":
                return Math.max(1, TimeUnit.DAYS.toMillis(number));
            default:
                return Long.valueOf(time);
        }
    }

    public static void configureMDC(final Exchange exchng) {
        String uuid = exchng.getProperty(Constants.TRACER_ID, String.class);
        if (StringUtils.hasText(uuid))
            MDC.put(Constants.TRACER_ID, uuid);
    }

    public static int isMenorIdade(String birthday) {
        if (!StringUtils.hasText(birthday))
            return 2;
        try {
            return isMenorIdade(LocalDate.parse(birthday, DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        } catch (DateTimeParseException e) {
            return isMenorIdade(LocalDateTime.parse(birthday, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        }
    }

    public static int isMenorIdade(LocalDateTime birthday) {
        return isMenorIdade(birthday.toLocalDate());
    }

    /*public static boolean isMenorIdade(LocalDate birthday) {
        if(birthday != null){
            return birthday.until(LocalDate.now(), ChronoUnit.YEARS) < 16;
        }
        return false;
    }*/
    public static int isMenorIdade(LocalDate birthday) {
        if (birthday != null) {
            if (birthday.until(LocalDate.now(), ChronoUnit.YEARS) < 16) {
                return 0;
            } else if (birthday.until(LocalDate.now(), ChronoUnit.YEARS) < 18) {
                return 1;
            } else return 2;
        }
        return 2;
    }

    public static boolean isBlankRow(Object obj) {
        boolean blankRow = false;
        if (obj instanceof Map) {
            Map<String, Object> map = ((Map<String, Object>) obj);
            final int mapSize = map.size();
            final int countBlanks = (int) map.entrySet().stream().filter(m -> !StringUtils.hasText(m.getValue().toString())).count();
            blankRow = (mapSize == countBlanks);
        }

        return blankRow;
    }

    public static String toUSDate(LocalDate date) {
        return Optional.ofNullable(date)
                .map(it -> it.format(PATTERN_US_DATE))
                .orElse(null);
    }

    public static LocalDate toUSDate(String date) {
        return Optional.ofNullable(date)
                .filter(Util.hasTextPredicate)
                .map(it -> LocalDate.parse(it, PATTERN_US_DATE))
                .orElse(null);
    }

    public static String cleanStringOrNull(String value){
        return Optional.ofNullable(value)
                .filter(Util.hasTextPredicate)
                .orElse(null);
    }

    private static final Predicate<? super String> hasTextPredicate =
        it -> StringUtils.hasText(it) && !"\"\"".equals(it);

}
