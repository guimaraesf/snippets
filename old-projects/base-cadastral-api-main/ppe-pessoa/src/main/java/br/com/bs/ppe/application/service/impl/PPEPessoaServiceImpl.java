package br.com.bs.ppe.application.service.impl;

import br.com.bs.exception.CpfCnpjAbstractException;
import br.com.bs.exception.CpfCnpjSemResultadoException;
import br.com.bs.ppe.application.service.PPEPessoaService;
import br.com.bs.ppe.domain.service.PPEService;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class PPEPessoaServiceImpl implements PPEPessoaService {

    private final PPEService ppeService;

    public PPEPessoaServiceImpl(PPEService ppeService) {
        this.ppeService = ppeService;
    }

    @Override
    public Map<String, Object> findByCPFMap(String cpf) throws CpfCnpjAbstractException {
        return this.ppeService.findByCPF(cpf)
                .map(it -> it.toMap())
                .orElseThrow(() -> new CpfCnpjSemResultadoException());
    }

}
