package br.com.leonardozv.spark.streaming.exemplos.services;

import br.com.leonardozv.spark.streaming.exemplos.models.EventoEFinanceira;

public class EventoEFinanceiraService {

    public static EventoEFinanceira obterEvento(String codigoEventoEFinanceira) {
        return new EventoEFinanceira(codigoEventoEFinanceira, 1111111111);
    }

}
