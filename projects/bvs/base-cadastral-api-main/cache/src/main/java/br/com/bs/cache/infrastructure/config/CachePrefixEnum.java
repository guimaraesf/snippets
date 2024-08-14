package br.com.bs.cache.infrastructure.config;

public enum CachePrefixEnum {

    RATE_LIMITING("rate-limiting"),
    SERPRO("serpro"),
    SERPRO_CPF("serpro-cpf"),
    SERPRO_CNPJ("serpro-cnpj");

    private final String name;

    CachePrefixEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
