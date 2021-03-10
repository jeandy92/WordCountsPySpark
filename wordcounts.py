import sys
from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    # Criar SparkContext
    conf = SparkConf().setAppName("Conta Palavras").setMaster('spark://192.168.15.6')
    sc = SparkContext(conf=conf)

    # Carrega o arquivo
    palavras = sc.textFile("/tmp/input.txt").flatMap(lambda line: line.split(" "))

    # Conta a ocorrencia de palavras
    contagem = palavras.map(lambda palavra: (palavra, 1)).reduceByKey(lambda a, b: a + b)
    
    #Salvar o Resultado
    contagem.saveAsTextFile("/tmp/saida10")
