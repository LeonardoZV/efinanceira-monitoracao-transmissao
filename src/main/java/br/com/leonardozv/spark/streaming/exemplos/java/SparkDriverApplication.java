package br.com.leonardozv.spark.streaming.exemplos.java;

import br.com.leonardozv.spark.streaming.exemplos.java.efinanceira.jobs.CapturarEventosJob;
import br.com.leonardozv.spark.streaming.exemplos.java.efinanceira.jobs.ConsolidarBaseEventosJob;
import br.com.leonardozv.spark.streaming.exemplos.java.efinanceira.jobs.GerarRelatorioTransmissaoJob;
import br.com.leonardozv.spark.streaming.exemplos.java.eventdrivenledger.jobs.CaptureLedgerPostingsJob;
import br.com.leonardozv.spark.streaming.exemplos.java.eventdrivenledger.jobs.OptimizeLedgerPostingsJob;
import br.com.leonardozv.spark.streaming.exemplos.java.eventdrivenledger.jobs.ProjectDailyLedgerMovementJob;
import br.com.leonardozv.spark.streaming.exemplos.java.multivisao.jobs.RegrasEventoTransferenciaRealizadaJob;
import br.com.leonardozv.spark.streaming.exemplos.scala.eventdrivenledger.jobs.ProjectMonthlyLedgerBalanceJob;

public class SparkDriverApplication
{

	public static void main(String[] args) throws Exception
    {

		if (args.length == 0)
			throw new Exception("Necessário informar o Job a ser executado.");

    	switch(args[0]) {

    		// EFinanceira
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
			case "CaptureLedgerPostingsJob":
				CaptureLedgerPostingsJob.executar(args);
				break;

			case "OptimizeLedgerPostingsJob":
				OptimizeLedgerPostingsJob.executar(args);
				break;

			case "ProjectDailyLedgerMovementJob":
				ProjectDailyLedgerMovementJob.executar(args);
				break;

			case "ProjectMonthlyLedgerBalanceJob":
				ProjectMonthlyLedgerBalanceJob job = new ProjectMonthlyLedgerBalanceJob();
				job.executar(args);
				break;

			case "TesteJobJava":
				TesteJobJava.executar(args);
				break;

			default:
				throw new Exception("Não foi possível identificar o Job: " + args[0]);

		}

    }
    
}
