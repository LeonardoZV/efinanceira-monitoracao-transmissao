package br.com.leonardozv.spark.streaming.exemplos.java.udfs;

import br.com.leonardozv.spark.streaming.exemplos.java.efinanceira.services.EventoEFinanceiraService;
import org.apache.spark.sql.api.java.UDF1;

public class ObterCnpjEmpresaDeclaranteEFinanceira implements UDF1<String, Long> {

    private static final long serialVersionUID = 1L;

    @Override
    public Long call(String codigoEventoEFinanceira) {
        return EventoEFinanceiraService.obterEvento(codigoEventoEFinanceira).getNumeroCnpjEmpresaDeclarante();
    }

}
