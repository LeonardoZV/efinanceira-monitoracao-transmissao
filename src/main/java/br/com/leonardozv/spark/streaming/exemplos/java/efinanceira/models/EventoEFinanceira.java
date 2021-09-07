package br.com.leonardozv.spark.streaming.exemplos.java.efinanceira.models;

public class EventoEFinanceira {

    private String codigoEventoEFinanceira;
    private long numeroCnpjEmpresaDeclarante;

    public EventoEFinanceira(String codigoEventoEFinanceira, long numeroCnpjEmpresaDeclarante) {
        this.codigoEventoEFinanceira = codigoEventoEFinanceira;
        this.numeroCnpjEmpresaDeclarante = numeroCnpjEmpresaDeclarante;
    }

    public String getCodigoEventoEFinanceira() {
        return codigoEventoEFinanceira;
    }

    public void setCodigoEventoEFinanceira(String codigoEventoEFinanceira) {
        this.codigoEventoEFinanceira = codigoEventoEFinanceira;
    }

    public long getNumeroCnpjEmpresaDeclarante() {
        return numeroCnpjEmpresaDeclarante;
    }

    public void setNumeroCnpjEmpresaDeclarante(long numeroCnpjEmpresaDeclarante) {
        this.numeroCnpjEmpresaDeclarante = numeroCnpjEmpresaDeclarante;
    }

}
