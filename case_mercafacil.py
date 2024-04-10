#Script Case Mercafacil - Amanda Louise Costa Nascimento
import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from google.cloud import storage
from pyspark.sql import functions as F


def start_or_create_spark():

    # Inicializar sessão Spark
    spark = SparkSession.builder \
        .appName("Case Mercafacil Amanda Nascimento") \
        .config("spark.jars", "/usr/lib/hadoop/lib/gcs-connector-hadoop3-latest.jar") \
        .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar') \
        .getOrCreate()
        
    return spark

def read_file(bucket_name, pasta, formato):
    # Lista para armazenar os DataFrames lidos
    dfs = []

    # Listar blobs no bucket
    blobs = list(client.list_blobs(bucket_name, prefix=pasta))

    # Para cada blob na lista de blobs
    for blob in blobs:
        # Obter o nome do blob
        nome_blob = blob.name
        print(nome_blob)
        # Ignorar pasta raiz
        if nome_blob == pasta:
            continue

        print('Lendo arquivo: ' + nome_blob)

        # Ler o arquivo como DataFrame Spark
        df = spark.read.format(formato).option('header', 'true').option('inferSchema', 'true').option('sep', ';').load(f'gs://{bucket_name}/{nome_blob}')
        
        # Adicionar DataFrame à lista
        dfs.append(df)

    # Concatenar todos os DataFrames em um único DataFrame
    df_final = dfs[0]
    for df_temp in dfs[1:]:
        df_final = df_final.union(df_temp)
    
    return df_final

def tratamentos_vendas(df):

    df = df.withColumn('COD_ID_LOJA', col('COD_ID_LOJA').cast(IntegerType()))

    # Remover dígitos à esquerda até o '|' do campo COD_ID_VENDA_UNICO e transformar em inteiro
    df = df.withColumn('COD_ID_VENDA_UNICO', regexp_extract(col('COD_ID_VENDA_UNICO'), r'\|(\d+)', 1).cast(IntegerType()))

    # Convertendo o campo 'NUM_ANOMESDIA' para o tipo de data 'yyyy-MM-dd'
    df = df.withColumn('DAT_ANOMESDIA', to_date(col('NUM_ANOMESDIA').cast('string'), 'yyyyMMdd')).drop('NUM_ANOMESDIA')
    # Formatando o campo de data para 'yyyy-MM-dd'
    df = df.withColumn('DAT_ANOMESDIA', to_date(date_format('DAT_ANOMESDIA', 'yyyy-MM-dd')))
    
    return df

def tratamentos_produtos(df):
    
    # Abertura do array e conversão para inteiro
    df = df.withColumn('ARR_CATEGORIAS_PRODUTO', split(df['ARR_CATEGORIAS_PRODUTO'], ',').cast(ArrayType(StringType())))
    df = df.withColumn("ARR_CATEGORIAS_PRODUTO", explode("ARR_CATEGORIAS_PRODUTO"))
    df = df.withColumn("COD_CATEGORIAS_PRODUTO", regexp_replace("ARR_CATEGORIAS_PRODUTO", "[\[\]\']", "").cast(IntegerType()))
    df = df.drop("ARR_CATEGORIAS_PRODUTO")
    
    return df

def tratamentos_clientes(df):
    
    #Removendo hora da coluna DAT_DATA_NASCIMENTO
    df = df.withColumn('DAT_DATA_NASCIMENTO', date_format(col('DAT_DATA_NASCIMENTO'), 'yyyy-MM-dd'))
    
    return df


def write_bigquery(dataframe, bq_dataset, bq_table, gcs_tmp_bucket):

    # spark.conf.set('temporaryGcsBucket', gcs_tmp_bucket)
    dataframe.write \
        .format("bigquery") \
        .option("table", "{}.{}".format(bq_dataset, bq_table)) \
        .option("temporaryGcsBucket", gcs_tmp_bucket) \
        .mode('append') \
        .save()

    return None

def write_storage(dataframe, bucket, file_name):
    dataframe.write.format('parquet') \
    .mode('overwrite') \
    .save('gs://{}/agregacoes/{}.parquet'.format(bucket, file_name))

    return None

def main(bucket_name, proj_name, bq_dataset, table_bq):
    try:
        spark = start_or_create_spark()
        # Configurar projeto do Google Cloud
        projeto = proj_name
        os.environ['GOOGLE_CLOUD_PROJECT'] = projeto
        # Definir nome do bucket 
        bucket = bucket_name

        # Configurar o conector GCS para o Spark
        spark.conf.set('google.cloud.auth.service.account.enable', 'true')
        spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false") #Evitando problemas de compatibilidade

        # Inicializando client do cloud storage
        client = storage.Client()

        # Lendo o arquivo de vendas do bucket e aplicando tratamentos
        df_venda = read_file(bucket_name, 'vendas/', 'csv')
        df_venda = tratamentos_vendas(df_venda)

        # Lendo o arquivo de produtos do bucket e aplicando tratamentos
        df_produtos = read_file(bucket_name, 'produtos/', 'parquet')
        df_produtos = tratamentos_produtos(df_produtos)

        # Lendo o arquivo de clientes do bucket e aplicando tratamentos
        df_clientes = read_file(bucket_name, 'clientes/', 'parquet')
        df_clientes = tratamentos_clientes(df_clientes)

        # Lendo o arquivo de categorias de produtos do bucket
        df_cat = read_file(bucket_name, 'categorias_produtos/categorias_produtos/', 'parquet')


        df_joined = df_venda.join(df_produtos, 'COD_ID_PRODUTO', 'inner') \
                   .join(df_clientes, ['COD_ID_CLIENTE', 'DES_SEXO_CLIENTE', 'DES_TIPO_CLIENTE'], 'inner') \
                   .join(df_cat, 'COD_ID_CATEGORIA_PRODUTO', 'inner').dropDuplicates()

        write_bigquery(df_joined, bq_dataset, table_bq, gcs_tmp_bucket)


        # Agrupando e agregando as vendas por produto
        df_vendas_agg = df_joined.groupBy('COD_ID_PRODUTO', 'DES_PRODUTO').agg(
            F.count('COD_ID_VENDA_UNICO').alias('AGG_VENDAS_POR_PRODUTO'))

        # Calculando o valor total das transações por COD_ID_LOJA
        valor_total_transacoes_por_loja = df_joined.groupBy('COD_ID_LOJA').agg(
            F.sum('VAL_VALOR_COM_DESC').alias('valor_total_transacoes'))

        # Agrupando e somando os itens por transação
        soma_itens = df_venda.groupBy('COD_ID_VENDA').agg(
            F.sum('VAL_VALOR_SEM_DESC').alias('soma_itens'))

        # Agrupando e somando os cupons por transação
        soma_cupons = df_venda.groupBy('COD_ID_VENDA').agg(
            F.sum('VAL_VALOR_COM_DESC').alias('soma_cupons'))

        # Calculando a diferença entre a soma dos itens e a soma dos cupons por transação
        diferenca = soma_itens.join(soma_cupons, 'COD_ID_VENDA') \
                            .withColumn('diferenca', col('soma_itens') - col('soma_cupons'))

        # Adicionando um campo de validação de transação. Se houver diferença, será preenchido com 'NOT OK'
        validacao = validacao.withColumn('STATUS_TRANSACAO',
                                        when(col('diferenca') == 0, 'OK').otherwise('NOT OK'))
        
        # Calculando o faturamento por COD_ID_CLIENTE
        faturamento_por_cliente = df_venda.groupBy('COD_ID_CLIENTE').agg(
            sum('VAL_VALOR_COM_DESC').alias('faturamento'))

        # Identificando os produtos que o cliente nunca comprou
        prods_nunca_comprados = df_produtos.join(df_venda, on='COD_ID_PRODUTO', how='left_anti')

        # Calculando as vendas desses produtos
        vendas_prods_nunca_comprados = prods_nunca_comprados.groupBy('COD_ID_PRODUTO', 'COD_ID_CLIENTE').agg(
            sum('VAL_VALOR_COM_DESC').alias('VENDAS'))

        # Classificando os produtos com base nas vendas e selecionando os 5 melhores
        top_5_melhores_produtos = vendas_prods_nunca_comprados.orderBy(desc('VENDAS')).limit(5)

        #Carregando arquivos com agregações geradas no bucket do storage
        write_storage(df_vendas_agg, bucket, 'vendas_por_produto')
        write_storage(faturamento_por_cliente, bucket, 'faturamento_por_cliente')
        write_storage(vendas_prods_nunca_comprados, bucket, 'vendas_prods_nunca_comprados')
        write_storage(top_5_melhores_produtos, bucket, 'top_5_melhores_produtos')

        return df
    except Exception as ex:
        print(ex)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--bucket_name',
        type=str,
        dest='bucket_name',
        required=True,
        help='Bucket do GCS')

    parser.add_argument(
        '--proj_name',
        type=str,
        dest='proj_name',
        required=True,
        help='Nome do projeto da GCP')

    parser.add_argument(
        '--bq_dataset',
        type=str,
        dest='bq_dataset',
        required=True,
        help='Dataset do BigQuery')

    parser.add_argument(
        '--table_bq',
        type=str,
        dest='table_bq',
        required=True,
        help='Tabela do BigQuery Destino')

    known_args, pipeline_args = parser.parse_known_args()

    main(bucket_name=known_args.bucket_name,
         proj_name=known_args.proj_name,
         bq_dataset=known_args.bq_dataset,
         table_bq=known_args.table_bq,
         gcs_tmp_bucket="case_mercafacil/temp_files"
         )