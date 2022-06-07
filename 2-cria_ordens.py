import sqlite3
import time
import sys
from myfuncs import *
import numpy as np
import pandas as pd
from pandas_datareader import data as wb
import os
from datetime import date, datetime, timedelta

def cotacao_dolar(conn):

    c_cotdolar_ins = "INSERT INTO cotacao_dolar (dt_cotacao, cotacao_compra, cotacao_venda) VALUES (?, ?, ?)"
    c_rend_qry = "SELECT ticker, qty, vunit from custodia where tipo_ir = 'FII' and qty > 0 order by ticker"
    c_vat_qry = "SELECT id, ticker, tipo, data_com FROM rendimentos WHERE vatual = -1 and data_com is not null ORDER BY data_com"
    time_start = time.time()
   
    bcb_url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='{}'&@dataFinalCotacao='{}'&$top=100&$format=json"
    
    c = conn.cursor()
    c.execute('select max(dt_cotacao) from cotacao_dolar')
    dt_last=c.fetchone()
    dt_inicio = datetime.date(datetime.strptime(dt_last[0], '%Y-%m-%d')) + timedelta(days=1)
    dt_fim = date.today()
    if (dt_fim < dt_inicio + timedelta(days=30)): dt_meio = dt_fim
    else: dt_meio = dt_inicio + timedelta(days=30)

    while dt_fim >= dt_inicio:
       df = pd.read_json(bcb_url.format(dt_inicio.strftime('%m-%d-%Y'), dt_meio.strftime('%m-%d-%Y')))
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

    time_end = time.time()
    print(time_end - time_start)
    return None


def main(argv):

    if len(argv) < 1 or 'help' in argv:
        print('Uso:')
        print('\t2-cria_ordens.py <Database>')
        print('\t<Database>: Arquivo de banco a ser usado')
        sys.exit(2)

    print('Opening database ',argv[0])
    conn = sqlite3.connect(argv[0])
    c = conn.cursor()
       
#    conn = sqlite3.connect('investimentos.db')
#    c = conn.cursor()
    c_ord_ins = "INSERT INTO ordens(id, id_corretora, num_ordem, ticker, operacao, qty, vunit, vunit_d, vneg, vneg_d, corretagem, liquidacao, emolumentos, vfinal, vfinal_d, data_mov, tipo_ir, motivo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    c_entord_qry = "SELECT id, id_corretora, num_ordem, ticker, operacao, qty, vunit, data_mov, tipo_ir, motivo from ent_ord where computado = 0"
    c_corretora_qry = "SELECT id, corr_aca, corr_fii, corr_etf, corr_fip, corr_bdr from corretora"
    c_cotdolar_qry = "SELECT cotacao_compra, cotacao_venda FROM cotacao_dolar where dt_cotacao = ?"
    time_start = time.time()
    print("Creating Orders...", end=' ', flush=True)

    # Carrega tabela de corretoras

    try:
        c.execute(c_corretora_qry)
    except:
        print('Erro ao abrir o arquivo ',argv[0])
        print(len(argv),'\t:\t',argv)
        sys.exit(2)

    rows = c.fetchall()
    corr_val = {}
#    corr_tipo = {}
    lista = ['D','A','S','G','R']
    for corr in rows:
        corr_tipo = {}
        corr_tipo['ACA'] = corr[1]
        corr_tipo['FII'] = corr[2]
        corr_tipo['ETF'] = corr[3]
        corr_tipo['FIP'] = corr[4]
        corr_tipo['BDR'] = corr[5]
        corr_val[corr[0]] = corr_tipo
#        print(corr[0], type(corr[0]), corr_val[corr[0]], corr_val[corr[0]]['ACA'])
#    sys.exit(2)

    # Carrega tabela de ordens executadas
    c.execute(c_entord_qry)
    rows = c.fetchall()
    for ord_row in rows:
        corr_id = int(ord_row[1])
        ordem = ord_row[2]
        ticker = ord_row[3]
        operacao = ord_row[4]
        qty = ord_row[5]
        vunit = ord_row[6]
        dtneg = ord_row[7]
        tipo = ord_row[8]
        motivo = ord_row[9]

        #vneg = qty * vunit

        # C = Compra
        # V = Venda
        # D = Desdobramento
        # A = Amortização
        # G = Grupamento
        # S = Subscrição
        # R = Redução de Capital
        if operacao == 'C': v = 1
        elif operacao == 'V': v = -1
        elif operacao in lista: v = 0
       
        if tipo in ['ACA', 'BDR', 'FII', 'FIP', 'ETF']:
            vneg = qty * vunit
#            print(corr_val[corr_id][tipo], corr_id, tipo, corr_val[corr_id])
            corretagem = corr_val[corr_id][tipo] * v
            liquidacao = round((vneg * 0.00025) * v, 4)
            emolumentos = round((vneg * 0.00005) * v, 4)
            vunit_d = 0
            vneg_d = 0
        elif tipo == 'STK':
            vunit_d = vunit
            c.execute(c_cotdolar_qry, (dtneg, ))
            cotdolar_row = c.fetchone()
            try:
                print('STK', ord_row[6], cotdolar_row)
                cotdolar_compra = cotdolar_row[0]   ## Menor
                cotdolar_venda  = cotdolar_row[1]   ## Maior
            except TypeError as err:
#                cotacao_dolar((argv[0], ))
                cotacao_dolar(conn)
                c.execute(c_cotdolar_qry, (dtneg, ))
                cotdolar_row = c.fetchone()
                print('STK', ord_row[6], cotdolar_row)
                cotdolar_compra = cotdolar_row[0]   ## Menor
                cotdolar_venda  = cotdolar_row[1]   ## Maior
            corretagem = corr_val[corr_id]['ACA'] * v
            if operacao == 'V': vunit = vunit * cotdolar_compra
            else: vunit = vunit * cotdolar_venda
            vneg = qty * vunit
            vneg_d = qty * vunit_d
            liquidacao = 0
            emolumentos = 0
        vfinal = round(vneg + corretagem + liquidacao + emolumentos, 4)
        vfinal_d = round(vneg_d + corretagem + liquidacao + emolumentos, 4)

        try:
           #print(ordem, ticker, operacao, qty, vunit, vneg, abs(corretagem), abs(liquidacao), abs(emolumentos), vfinal, dtneg, tipo, motivo)
            c.execute(c_ord_ins, (None, corr_id, ordem, ticker, operacao, qty, round(vunit,4), round(vunit_d,4), round(vneg, 4), round(vneg_d, 4), abs(corretagem), abs(liquidacao), abs(emolumentos), vfinal, vfinal_d, dtneg, tipo, motivo))
            c.execute("UPDATE ent_ord SET computado = 1 where id = ?", (ord_row[0], ))
            conn.commit()
        except sqlite3.Error as err:
            conn.rollback()
            print('SQL ERROR: - Erro {!r} encontrado, errno e {}'.format(err, err.args[0]) + '\n')
            print(''.join('{} {} {} {} {} {} {} {} {} {} {} {} {}'.format(ordem, ticker, operacao, qty, vunit, vneg, corretagem, liquidacao, emolumentos, vfinal, dtneg, tipo, motivo))+'\n')
       
    conn.close()
    time_end = time.time()
    print(time_end - time_start)
    return None
    
if __name__ == "__main__":
  main(sys.argv[1:]);
