package br.com.leonardozv.spark.streaming.exemplos;

import br.com.leonardozv.spark.streaming.exemplos.jobs.efinanceiramonitoracaotransmissao.CapturarEventosJob;
import br.com.leonardozv.spark.streaming.exemplos.jobs.efinanceiramonitoracaotransmissao.ConsolidarBaseEventosJob;
import br.com.leonardozv.spark.streaming.exemplos.jobs.efinanceiramonitoracaotransmissao.GerarRelatorioTransmissaoJob;
import br.com.leonardozv.spark.streaming.exemplos.jobs.eventdrivenledger.AggregateAccountingDailyMovementJob;
import br.com.leonardozv.spark.streaming.exemplos.jobs.multivisao.RegrasEventoTransferenciaRealizadaJob;

public class SparkDriverApplication
{

	public static void main(String[] args) throws Exception
    {

		if (args.length == 0)
			throw new Exception("Necessário informar o Job a ser executado.");

    	switch(args[0]) {

    		// Efinanceira Monitoração Transmissão
			case "CapturarEventosJob":
				CapturarEventosJob.executar(args);
				break;

			case "ConsolidarBaseEventosJob":
				ConsolidarBaseEventosJob.executar(args);
				break;

			case "GerarRelatorioTransmissaoJob":
				GerarRelatorioTransmissaoJob.executar(args);
				break;

			// Multivisão
			case "RegrasEventoTransferenciaRealizadaJob":
				RegrasEventoTransferenciaRealizadaJob.executar(args);
				break;

			// Event Driven Ledger
			case "AggregateAccountingDailyMovementJob":
				AggregateAccountingDailyMovementJob.executar(args);
				break;

			default:
				throw new Exception("Não foi possível identificar o Job: " + args[0]);

		}

    }
    
}
