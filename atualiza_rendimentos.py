import sqlite3
import time
from myfuncs import *
import sys
import numpy as np
import pandas as pd
from pandas_datareader import data as wb
import os
from datetime import date, datetime

def main(argv):
#    conn = sqlite3.connect('investimentos.db')
#    c = conn.cursor()

    c_rend_ins = "INSERT INTO rendimentos (ticker, tipo, anomes, vunit, qty) VALUES (?, ?, ?, ?, ?)"
    c_rend_upd = "UPDATE rendimentos SET vatual = ? WHERE id = ?"
    c_rend_qry = "SELECT ticker, qty, vunit from custodia where tipo_ir = 'FII' and qty > 0 order by ticker"
    c_vat_qry = "SELECT id, ticker, tipo, data_com FROM rendimentos WHERE vatual = -1 and data_com is not null ORDER BY data_com"
    time_start = time.time()
    #print("Calculando...", end=' ', flush=True)
    print("Calculando...", flush=True)

    if len(sys.argv[1:]) < 2:
           print('Uso:')
           print('\trendfii.py <Database> <Argumentos>')
           print('Argumentos:')
           print('\t<database>: Banco de dados a ser utilizado')
           print('\t<anomes>: Ano+Mês de entrada dos rendimentos de FII')
           sys.exit(2)

    anomes = argv[1]
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

    if anomes.isnumeric():

       rows = c.fetchall()

       for vrend_row in rows:
          ticker = vrend_row[0]
          qty = float(vrend_row[1])
          vunit = round(float(vrend_row[2]),2)
          try:
              print(ticker, anomes, vunit, qty)
              c.execute(c_rend_ins, (ticker, 'FII', anomes, vunit, qty))
              conn.commit()
          except sqlite3.Error as err:
    #         conn.rollback()
             print('SQL ERROR: - Erro {!r} encontrado, errno e {}'.format(err, err.args[0]) + '\n')
             print(''.join('{} {} {} {}'.format(ticker, anomes, vunit, qty))+'\n')
    else: print('Anomes invalido: ',anomes)

    prices=pd.DataFrame()
    c.execute(c_vat_qry)
    rows = c.fetchall()

    for vat_row in rows:
       vat_id = vat_row[0]
       vat_ticker = vat_row[1]
       vat_tipo = vat_row[2]
       if vat_tipo == 'STK': vat_ya_ticker = vat_ticker
       else: vat_ya_ticker = vat_ticker+'.SA'       
       vat_dt_com = datetime.date(datetime.strptime(vat_row[3], '%Y-%m-%d'))
    #   print(vat_ya_ticker, vat_dt_com)
       try:
          cotacao = wb.DataReader(vat_ya_ticker, data_source='yahoo', start=vat_dt_com, end=vat_dt_com)
       except:
          print('Data', vat_dt_com,'para ticker',vat_ya_ticker,'nao encontrado!')
    #      valor = -1
          continue

    #   prices[vat_ya_ticker] = cotacao["Adj Close"]   
       prices = cotacao["Adj Close"]   

    #   print("Cotacao", cotacao["Adj Close"])
       
       if prices.isnull().values.any(): valor = -1
       else: valor = float(prices[0])
    #   print(vat_ya_ticker, vat_dt_com, valor)
    #   print("Prices", prices, prices[0], valor)
       
       try:
          c.execute(c_rend_upd, (round(valor,2), vat_id))
          conn.commit()
       except sqlite3.Error as err:
          conn.rollback()
          print('SQL ERROR: - Erro {!r} encontrado, errno e {}'.format(err, err.args[0]) + '\n')
          print(''.join('{} {}'.format(prices, vat_id))+'\n')
             
       
    conn.close()
    time_end = time.time()
    print(time_end - time_start)
    return None
    
if __name__ == "__main__":
  main(sys.argv[1:]);


    # https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpj=26614291000100
    # https ://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo?$format=json&dataInicial='01-01-2021'&dataFinalCotacao='01-31-2021'
    #https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='01-01-2021'&@dataFinalCotacao='01-31-2021'&$top=100&$format=json
