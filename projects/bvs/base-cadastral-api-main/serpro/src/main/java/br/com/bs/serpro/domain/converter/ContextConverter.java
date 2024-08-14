package br.com.bs.serpro.domain.converter;

import java.util.Map;

public interface ContextConverter<T> {

    Map<String, Object> toMap(T data);

    boolean isApplicable(String context);

}
