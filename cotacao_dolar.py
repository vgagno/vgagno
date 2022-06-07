import sqlite3
import time
from myfuncs import *
import sys
import numpy as np
import pandas as pd
from pandas_datareader import data as wb
import os
from datetime import date, datetime, timedelta

def cotacao_dolar(argv):
#    conn = sqlite3.connect('investimentos.db')
#    c = conn.cursor()

    c_cotdolar_ins = "INSERT INTO cotacao_dolar (dt_cotacao, cotacao_compra, cotacao_venda) VALUES (?, ?, ?)"
    c_rend_qry = "SELECT ticker, qty, vunit from custodia where tipo_ir = 'FII' and qty > 0 order by ticker"
    c_vat_qry = "SELECT id, ticker, tipo, data_com FROM rendimentos WHERE vatual = -1 and data_com is not null ORDER BY data_com"
    time_start = time.time()
    #print("Calculando...", end=' ', flush=True)
    print("Calculando...", flush=True)

    if len(argv) == 0:
           print('Uso:')
           print('\trendfii.py <Database> <Argumentos>')
           print('Argumentos:')
           print('\t<database>: Banco de dados a ser utilizado')
           sys.exit(2)

    print('Opening database ',argv[0])
    print(len(argv),'\t:\t',argv)
    conn = sqlite3.connect(argv[0])
    c = conn.cursor()     

    try:
       c.execute(c_rend_qry)
    except:
       print('Erro ao abrir o arquivo ',argv[0])
#       print(len(argv),'\t:\t',argv)
       sys.exit(2)
	   
    bcb_url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='{}'&@dataFinalCotacao='{}'&$top=100&$format=json"
    c.execute('select max(dt_cotacao) from cotacao_dolar')
    dt_last=c.fetchone()
    dt_inicio = datetime.date(datetime.strptime(dt_last[0], '%Y-%m-%d')) + timedelta(days=1)
    dt_fim = date.today()
    if (dt_fim < dt_inicio + timedelta(days=30)): dt_meio = dt_fim
    else: dt_meio = dt_inicio + timedelta(days=30)

#    print(dt_inicio.strftime('%m-%d-%Y'), dt_meio.strftime('%m-%d-%Y'), dt_fim.strftime('%m-%d-%Y'))
    while dt_fim >= dt_inicio:
#       print(bcb_url.format(dt_inicio.strftime('%m-%d-%Y'), dt_meio.strftime('%m-%d-%Y')))
       df = pd.read_json(bcb_url.format(dt_inicio.strftime('%m-%d-%Y'), dt_meio.strftime('%m-%d-%Y')))
#       print(df)
       for linha in df['value']:
          cotacaocompra = round(linha['cotacaoCompra'],4)
          cotacaovenda = round(linha['cotacaoVenda'],4)
          datacotacao = linha['dataHoraCotacao'][:10]
          print(datacotacao, cotacaocompra, cotacaovenda)
          try:
             c.execute(c_cotdolar_ins, (datacotacao, cotacaocompra, cotacaovenda))
             conn.commit()
          except sqlite3.Error as err:
             conn.rollback()
             print('SQL ERROR: - Erro {!r} encontrado, errno e {}'.format(err, err.args[0]) + '\n')
             print(''.join('{} {} {}'.format(datacotacao, cotacaocompra, cotacaovenda))+'\n')
             
       dt_inicio = dt_meio + timedelta(days=1)
       if (dt_fim < dt_inicio + timedelta(days=30)): dt_meio = dt_fim
       else: dt_meio = dt_inicio + timedelta(days=30)

    conn.close()
    time_end = time.time()
    print(time_end - time_start)
    return None


if __name__ == "__main__":
  cotacao_dolar(sys.argv[1:]);


#{"@odata.context":"https://was-p.bcnet.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata$metadata#_CotacaoDolarPeriodo",
#"value":[{"cotacaoCompra":5.16200,"cotacaoVenda":5.16260,"dataHoraCotacao":"2021-01-04 13:07:33.461"},
#{"cotacaoCompra":5.32630,"cotacaoVenda":5.32690,"dataHoraCotacao":"2021-01-05 13:11:14.045"},
#{"cotacaoCompra":5.31760,"cotacaoVenda":5.31820,"dataHoraCotacao":"2021-01-06 13:12:28.251"},
#{"cotacaoCompra":5.34270,"cotacaoVenda":5.34330,"dataHoraCotacao":"2021-01-07 13:11:33.564"}]}
#https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='2022-02-26'&@dataFinalCotacao='2022-03-05'&$top=100&$format=json