Tarefa: Desenvolvimento de um Pipeline ETL em Ambiente Cloud


Contexto:

Imagine que você está trabalhando em uma empresa do varejo e que precisa analisar grandes volumes de dados de transações para fornecer insights de negócios. A empresa utiliza uma arquitetura baseada em nuvem (Google Cloud).


Requisitos:

Fonte de Dados:

Os dados de transações estão armazenados em arquivos Parquet e CSV no Google Cloud Storage: dados desafio mercafacil

Descrição dos datasets: descricao_datasets


Transformação:

Desenvolva um pipeline ETL que realiza as seguintes transformações nos dados utilizando pySpark:

Conversão de tipos de dados adequados.

Deduplicação de dados.

Agregação de vendas por produto.

Cálculo do valor total das transações, lembre-se de validar a soma dos itens x soma dos cupons

Como resultado, além do faturamento, gostaríamos da lista dos 5 melhores produtos que o cliente nunca comprou (modelo de upsell de produtos)


Destino de Dados:

Os resultados das transformações devem ser armazenados em uma tabela ou conjunto de arquivos em um banco de dados de sua preferência.


Agendamento e Orquestração:

Implemente uma lógica de agendamento para que o pipeline seja executado automaticamente em intervalos regulares.

Considere a orquestração do pipeline para garantir a execução ordenada das etapas, aqui na Mercafacil utilizamos o Airflow, que será uma ferramenta no seu dia a dia.


Monitoramento:

Integre algum tipo de monitoramento para registrar métricas, erros e o desempenho do pipeline, nesse requisito, pode ser o Airflow para monitorar sua DAG por exemplo, a geração de alertas em caso de falha, sejam criativos!


Observações Adicionais:

Documente a sua solução da forma que achar melhor, pense que nela você precisa fornecer explicações claras sobre as escolhas de arquitetura, código, variáveis de ambiente, possíveis falhas. Em caso de dúvidas não deixem de nos acionar.
