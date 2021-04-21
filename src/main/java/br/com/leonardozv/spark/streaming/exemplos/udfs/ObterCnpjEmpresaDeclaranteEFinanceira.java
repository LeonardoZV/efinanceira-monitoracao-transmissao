package br.com.leonardozv.spark.streaming.exemplos.udfs;

import br.com.leonardozv.spark.streaming.exemplos.services.EventoEFinanceiraService;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.spark.sql.api.java.UDF1;

import java.nio.ByteBuffer;

public class ObterCnpjEmpresaDeclaranteEFinanceira implements UDF1<String, Long> {

    private static final long serialVersionUID = 1L;

    @Override
    public Long call(String codigoEventoEFinanceira) {
        return EventoEFinanceiraService.obterEvento(codigoEventoEFinanceira).getNumeroCnpjEmpresaDeclarante();
    }

}
