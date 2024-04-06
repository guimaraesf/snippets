package br.com.bs.ppe.application.service;

import br.com.bs.exception.CpfCnpjAbstractException;

import java.util.Map;

public interface PPEPessoaService {

    Map<String, Object> findByCPFMap(String cpf) throws CpfCnpjAbstractException;

}
