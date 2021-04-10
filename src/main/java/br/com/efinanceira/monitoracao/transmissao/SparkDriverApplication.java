package br.com.efinanceira.monitoracao.transmissao;

import br.com.efinanceira.monitoracao.transmissao.jobs.CapturarEventosJob;
import br.com.efinanceira.monitoracao.transmissao.jobs.GerarRelatorioTransmissaoJob;

public class SparkDriverApplication
{

	public static void main(String[] args) throws Exception
    {

		if (args.length == 0)
			throw new Exception("Necessário informar o Job a ser executado.");

    	switch(args[0]) {

			case "CapturarEventosJob":
				CapturarEventosJob.executar(args);
				break;

			case "GerarRelatorioTransmissaoJob":
				GerarRelatorioTransmissaoJob.executar(args);
				break;

			default:
				throw new Exception("Não foi possível identificar o Job: " + args[0]);

		}

    }
    
}
