package br.com.leonardozv.spark.streaming.exemplos.java.efinanceira.services;

import br.com.leonardozv.spark.streaming.exemplos.java.efinanceira.models.EventoEFinanceira;

public class EventoEFinanceiraService {

    public static EventoEFinanceira obterEvento(String codigoEventoEFinanceira) {
        return new EventoEFinanceira(codigoEventoEFinanceira, 1111111111);
    }

}
