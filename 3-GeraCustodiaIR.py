import sqlite3
import time
import datetime
from myfuncs import *
import sys

def main(argv):

    if len(argv) < 2 or 'help' in argv:
       print('Uso:')
       print('\t3-GeraCustodiaIR.py <Database> <Argumentos>')
       print('\t<Database>: Arquivo de banco a ser usado')
       print('Argumentos:')
       print('\tregen: Regera custódia e tabela de IR')
       print('\tdiff: Acrescenta ordens recentes (10x mais rápido)')
       print(len(argv),'\t:\t',argv)
       sys.exit(2)

    print('Opening database ',argv[0])
    conn = sqlite3.connect(argv[0])
    c = conn.cursor()
       
    #c_ord_qry = "SELECT id, ticker, operacao, qty, vfinal, tipo_ir, data_mov, motivo from ordens where computado = 0 order by data_mov"
    #c_ord_qry = "SELECT id, ticker, operacao, qty, vfinal, tipo_ir, data_mov, motivo from ordens where strftime('%Y', data_mov) <> '2021' order by data_mov"
    c_cus_qry = "SELECT  qty, vunit, vunit_d from custodia where ticker = ?"
    c_cus_ins = "INSERT INTO custodia(id, ticker, tipo_ir, qty, vunit, vunit_d) VALUES (?, ?, ?, ?, ?, ?)"
    c_cus_upd = "UPDATE custodia SET qty = ?, vunit = ?, vunit_d = ? WHERE ticker = ?"
    c_cus_del = "DELETE FROM custodia WHERE ticker = ?"
    c_fiid_upd = "UPDATE fii_descricao SET ativo = ?, dt_out = ? WHERE ticker = ?"
    c_ir_apuracao_ins = "INSERT INTO ir_apuracao(id, anomes, tipo_ir, ganho, volume, irrf, ir_devido, saldo_ir) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    c_irenda_ins = "INSERT INTO irenda(id, ticker, anomes, tipo_ir, categoria, qty, vunit, ganho, volume, irrf, irdevido) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    c_irenda_qry = "SELECT anomes, tipo_ir, sum(ganho), sum(volume), sum(irrf), sum(irdevido) FROM irenda GROUP BY anomes,tipo_ir ORDER BY anomes, tipo_ir"
    c_histpm_ins = "INSERT INTO historico_pm (cliente, ticker, qty, vunit, vunit_d, data_mov) VALUES (?, ?, ?, ?, ?, ?)"
    c_cotdolar_qry = "SELECT cotacao_compra, cotacao_venda FROM cotacao_dolar where dt_cotacao = ?" 
#    c_op_qry = "SELECT id, ticker, opcao, qty, strike, premio, cust_m, cust_d, dt_venc, dt_reversao from opcoes where status <> 'A' and computado = 0"

    time_start = time.time()
    #c.execute("DELETE FROM custodia")
    #c.execute("DELETE FROM irenda")
    try:
        c.execute("DELETE FROM ir_apuracao")
        c.execute("DELETE FROM sqlite_sequence WHERE name='ir_apuracao'")
    except:
        print('Erro ao abrir o arquivo ',argv[0])
        print(len(argv),'\t:\t',argv)
        sys.exit(2)

    if 'diff' in argv:
        c_ord_qry = "SELECT id, ticker, operacao, qty, vfinal, vfinal_d, tipo_ir, data_mov, motivo from ordens where computado = 0 order by data_mov"
        c_op_qry = "SELECT id, ticker, opcao, condicao, qty, strike, pre_m, cus_m, pre_d, cus_d, dt_venc, dt_reversao, status from opcoes where status <> 'A' and computado = 0"
    elif 'regen' in argv:
        c_ord_qry = "SELECT id, ticker, operacao, qty, vfinal, vfinal_d, tipo_ir, data_mov, motivo from ordens order by data_mov"
#        c_ord_qry = "SELECT id, ticker, operacao, qty, vfinal, tipo_ir, data_mov, motivo from ordens where data_mov not like '2022%' order by data_mov"
        c_op_qry = "SELECT id, ticker, opcao, condicao, qty, strike, pre_m, cus_m, pre_d, cus_d, dt_venc, dt_reversao, status from opcoes where status <> 'A'"
        c.execute("DELETE FROM custodia")
        c.execute("DELETE FROM irenda")
        c.execute("DELETE FROM historico_pm")
        c.execute("DELETE FROM sqlite_sequence WHERE name in ('custodia', 'irenda', 'historico_pm')")
    else: 
        print('Parâmetro incorreto: ' + argv[0])
        sys.exit(2)

    c.execute(c_ord_qry)
    rows = c.fetchall()
    for ord_row in rows:
        ord_id = ord_row[0]
        ord_ticker = ord_row[1]
        ord_operacao = ord_row[2]
        ord_qty = ord_row[3]
        ord_vfinal = ord_row[4]
        ord_vfinal_d = ord_row[5]
        ord_tipo_ir = ord_row[6]
        ord_data_mov_dt = datetime.datetime.strptime(ord_row[7], '%Y-%m-%d')
        ord_motivo = ord_row[8]

        c.execute(c_cus_qry, (ord_ticker,))
        cus_row = c.fetchone()
        cus_dml = 'U'

        # if ord_tipo_ir == 'STK':
            # c.execute(c_cotdolar_qry, (ord_row[6], ))
            # cotdolar_row = c.fetchone()
            # print('STK', ord_row[6], cotdolar_row)
            # cotdolar_compra = cotdolar_row[0]   ## Menor
            # cotdolar_venda  = cotdolar_row[1]   ## Maior
            # if ord_operacao == 'V': ord_vfinal = ord_vfinal * cotdolar_compra
            # else: ord_vfinal = ord_vfinal * cotdolar_venda

        ord_vunit = ord_vfinal / ord_qty
        ord_vunit_d = ord_vfinal_d / ord_qty

        if ord_operacao == 'C':
            if cus_row is None: cus_dml = 'I'
            else:
                cus_qty = cus_row[0] + ord_qty
                cus_vunit = ((cus_row[0] * cus_row[1]) + (ord_vfinal)) / (cus_row[0] + ord_qty)
                cus_vunit_d = ((cus_row[0] * cus_row[2]) + (ord_vfinal_d)) / (cus_row[0] + ord_qty)
        elif ord_operacao == 'S':
            cus_qty = cus_row[0] + ord_qty
            cus_vunit = ((cus_row[0] * cus_row[1]) + (ord_vfinal)) / (cus_row[0] + ord_qty)
            cus_vunit_d = ((cus_row[0] * cus_row[2]) + (ord_vfinal_d)) / (cus_row[0] + ord_qty)

        elif ord_operacao == 'D':
            cus_qty = cus_row[0] * ord_qty
            cus_vunit = cus_row[1] / ord_qty
            cus_vunit_d = cus_row[2] / ord_qty

        elif ord_operacao == 'G':
            cus_qty = truncate(cus_row[0] / ord_qty, 0)
            cus_vunit = cus_row[1] * ord_qty
            cus_vunit_d = cus_row[2] * ord_qty

        elif ord_operacao == 'A' or ord_operacao == 'R':
            cus_qty = cus_row[0]
            cus_vunit = cus_row[1] - ord_vunit
            cus_vunit_d = cus_row[2] - ord_vunit_d

        elif ord_operacao == 'V':
            cus_qty = cus_row[0] - ord_qty
            cus_vunit = cus_row[1]
            cus_vunit_d = cus_row[2]

            if cus_qty == 0: cus_dml = 'D'
          
            ### Imposto de Renda por venda   
            ir_anomesref = ord_data_mov_dt.year * 100 + ord_data_mov_dt.month
            ir_tipo_ir = ord_tipo_ir
            ir_categoria = ord_tipo_ir
            ir_ganho = (ord_vunit - cus_vunit) * ord_qty
            ir_volume = ord_vfinal
            ir_irrf = ir_volume * 0.00005
            if ir_ganho <= 0:
                ir_devido = 0
                if ord_motivo.startswith('Exercício de Opção') or ir_tipo_ir in ['ETF','BDR']: 
                    ir_categoria = ir_tipo_ir
                    ir_tipo_ir = 'BEO'
            elif ir_ganho > 0:
                if ir_tipo_ir == 'FII': ir_devido = ir_ganho * 0.2
                elif (ir_tipo_ir in ['ETF','BDR']): 
                    ir_devido = ir_ganho * 0.15
                    ir_tipo_ir = 'BEO'
                elif ir_tipo_ir == 'STK': ir_devido = ir_ganho * 0.15
                elif ir_tipo_ir == 'ACA': 
                    ir_devido = ir_ganho * 0.15
                    if ord_motivo.startswith('Exercício de Opção'): ir_tipo_ir = 'BEO'
                elif ir_tipo_ir == 'FIP': ir_devido = 0

            try:
            #     c_irenda_ins = "INSERT INTO irenda(id, ticker, anomes, tipo_ir, categoria, qty, vunit, ganho, volume, irrf, irdevido) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

                c.execute(c_irenda_ins, (None, ord_ticker, ir_anomesref, ir_tipo_ir, ir_categoria, ord_qty, round(ord_vunit,4), round(ir_ganho,4), round(ir_volume,4), round(ir_irrf,4), round(ir_devido,4)))
    #            conn.commit()
            except sqlite3.Error as err:
    #            conn.rollback()
                print('SQL ERROR: - Erro {!r} encontrado, errno e {}'.format(err, err.args[0]) + '\n')
                print(''.join('{} {} {} {} {} {} {} {}'.format(ir_anomesref, ir_tipo_ir, ir_categoria, round(ir_ganho,4), round(ir_volume,4), round(ir_irrf,4), round(ir_devido,4)))+'\n')
         
        try:
            if cus_dml == 'U':
                c.execute(c_cus_upd, (cus_qty, round(cus_vunit,4), round(cus_vunit_d,4), ord_ticker))
                c.execute(c_histpm_ins, ('1', ord_ticker, cus_qty, round(cus_vunit,4), round(cus_vunit_d,4), ord_data_mov_dt))
#                if ord_tipo_ir == 'ACA' or ord_tipo_ir == 'STK': print(ord_ticker, cus_qty, round(cus_vunit,4), ord_data_mov_dt, sep='\t')
            elif cus_dml == 'I':
                c.execute(c_cus_ins, (None, ord_ticker, ord_tipo_ir, ord_qty, round(ord_vunit,4), round(ord_vunit_d,4)))
                c.execute(c_histpm_ins, ('1', ord_ticker, ord_qty, round(ord_vunit,4), round(ord_vunit_d,4), ord_data_mov_dt))
#                if ord_tipo_ir == 'ACA' or ord_tipo_ir == 'STK': print(ord_ticker, ord_qty, round(ord_vunit,4), ord_data_mov_dt, sep='\t')
            elif cus_dml == 'D':
                c.execute(c_cus_del, (ord_ticker, ))
#                c.execute(c_cus_upd, (cus_qty, round(ord_vunit,4), ord_ticker))
                c.execute(c_histpm_ins, ('1', ord_ticker, cus_qty, round(cus_vunit,4),  round(cus_vunit_d,4), ord_data_mov_dt))
                if ord_tipo_ir == 'FII': c.execute(c_fiid_upd, (0, ord_data_mov_dt, ord_ticker))
#                if ord_tipo_ir == 'ACA' or ord_tipo_ir == 'STK': print(ord_ticker, cus_qty, round(cus_vunit,4), ord_data_mov_dt, sep='\t')
#            elif cus_dml == 'D': c.execute(c_cus_del, (ord_ticker, ))
            c.execute('UPDATE ordens SET computado = 1 WHERE id = ?', (ord_id, ))
            conn.commit()
        except sqlite3.Error as err:
            conn.rollback()
            print('SQL ERROR: - Erro {!r} encontrado, errno e {}'.format(err, err.args[0]) + '\n')
            print(''.join('{} {} {}'.format(ord_ticker, ord_qty, ord_vunit))+'\n')

    # Apuração IR Opções
#   c_op_qry = "SELECT id, ticker, opcao, condicao, qty, strike, pre_m, cus_m, pre_d, cus_d, dt_venc, dt_reversao, status from opcoes where status <> 'A'"
    c.execute(c_op_qry)
    rows = c.fetchall()
    for op_row in rows:
    #   print(op_row)
        op_id = op_row[0]
        op_ticker = op_row[1]
        op_opcao = op_row[2]
        op_cond = op_row[3]
        op_quant = op_row[4]
        op_strike = op_row[5]
        op_pre_m = op_row[6]
        op_cus_m = op_row[7]
        op_pre_d = op_row[8]
        op_cus_d = op_row[9]

        op_premio = op_pre_m + op_pre_d
        op_custos = op_cus_m + op_cus_d
        op_premio_liq = op_premio - (op_custos / op_quant)

        if op_row[11] == '-': op_dt_encerramento = op_row[10]   ##datetime.datetime.strptime(op_row[10], '%Y-%m-%d')
        else: op_dt_encerramento = op_row[11]   ## datetime.datetime.strptime(op_row[11], '%Y-%m-%d')

        op_anomesref = datetime.datetime.strptime(op_dt_encerramento, '%Y-%m-%d').year * 100 +  datetime.datetime.strptime(op_dt_encerramento, '%Y-%m-%d').month

        if op_opcao == 'STK':
            c.execute(c_cotdolar_qry, (op_dt_encerramento, ))
            cotdolar_row = c.fetchone()
            print(op_dt_encerramento, cotdolar_row)
            cotdolar_compra = cotdolar_row[0]   ## Menor
            cotdolar_venda  = cotdolar_row[1]   ## Maior
            op_ticker = op_ticker + '_OP'
            if op_cond == 'L':
                op_volume = op_quant * op_premio * cotdolar_compra
                op_ganho = op_volume - (op_custos * cotdolar_compra)
            elif op_cond == 'T':
                op_volume = op_quant * op_premio * cotdolar_venda                
                op_ganho = op_volume - (op_custos * cotdolar_venda)
            op_tipo_ir = 'STK'
            op_categoria = 'STK'
            op_irrf = 0
        else: 
            op_volume = op_quant * op_premio
            op_ganho = op_volume - op_custos
            op_tipo_ir = 'BEO'
            op_categoria = 'OP'
            op_ticker = op_ticker[0:4] + op_opcao
            if op_pre_m > 0: op_irrf = ((op_pre_m * op_quant) - op_cus_m) * 0.00005
            else: op_irrf = ((op_pre_d * op_quant) - op_cus_d) * 0.00005
      
        op_status = op_row[12]
       
        ### Imposto de Renda por operação de opção 
       
        if op_ganho <= 0: op_devido = 0
        elif op_status == 'E': 
            op_devido = 0
            op_ganho = 0
        else: op_devido = op_ganho * 0.15
       
        try:
            c.execute(c_irenda_ins, (None, op_ticker, op_anomesref, op_tipo_ir, op_categoria, op_quant, round(op_premio_liq,4), round(op_ganho,4), round(op_volume,4), round(op_irrf,4), round(op_devido,4)))
#            c.execute(c_irenda_ins, (None, op_ticker, op_anomesref, op_tipo_ir, op_quant, op_strike, op_ganho, op_volume, op_irrf, op_devido))
            c.execute('UPDATE opcoes SET computado = 1 WHERE id = ?', (op_id, ))
            conn.commit()
        except sqlite3.Error as err:
            conn.rollback()
            print('SQL ERROR: - Erro {!r} encontrado, errno e {}'.format(err, err.args[0]) + '\n')
            print(''.join('{} {} {} {} {} {}'.format(op_anomesref, op_tipo_ir, op_ganho, op_volume, op_irrf, op_devido))+'\n')

    ### Apuração de Imposto de Renda MaM
    #c_irenda_qry = "SELECT anomes, tipo, sum(ganho), sum(volume), sum(irrf), sum(irdevido) FROM irenda GROUP BY anomes,tipo ORDER BY anomes"
    c.execute(c_irenda_qry)
    rows = c.fetchall()
    preju_fii = 0
    preju_aca = 0
    saldo_ir = 0
    preju = {}
    for irenda_row in rows:
    #    id = irenda_row[0]
        anomes = irenda_row[0]
        tipo_ir = irenda_row[1]
        categoria = irenda_row[1]
        ganho = round(irenda_row[2],4)
        volume = round(irenda_row[3],4)
        irrf = round(irenda_row[4],4)
        ir_devido = round(irenda_row[5],4)

        if tipo_ir == 'ACA':
            if volume <= 20000 or ganho < 0: ir_devido = 0
        elif tipo_ir == 'BEO' and ganho < 0: ir_devido = 0
        elif tipo_ir == 'FII': ir_devido = 0
        elif tipo_ir == 'STK': 
            if volume <= 35000 or ganho < 0: ir_devido = 0
        
        if tipo_ir == 'FII':
            preju_fii = preju_fii + ganho - irrf
            if preju_fii > 0:
                ir_devido = round((preju_fii) * 0.15,4)
                preju_fii = 0
                saldo_ir = 0
            else: 
                saldo_ir = preju_fii
                ir_devido = 0
#            print(anomes, tipo_ir, ganho, volume, irrf, ir_devido, round(saldo_ir,4),sep='\t')               
        elif tipo_ir == 'BEO':
            preju_aca = preju_aca + ganho
            if preju_aca > 0:
                ir_devido = round((preju_aca) * 0.15,4) - irrf
                preju_aca = 0
            else: ir_devido = 0
            saldo_ir = preju_aca
#            print(anomes, tipo_ir, ganho, volume, irrf, ir_devido, round(saldo_ir,4),sep='\t')        
        elif tipo_ir == 'ACA':
            if ganho > 0:
                if volume <= 20000: preju_aca = preju_aca - irrf
                else:
                    preju_aca = preju_aca + ganho             
                    if preju_aca > 0:
                        ir_devido = round((preju_aca) * 0.15,4) - irrf
                        preju_aca = 0
                    else: ir_devido = 0
            else:
                preju_aca = preju_aca + ganho - irrf               
                ir_devido = 0
            saldo_ir = preju_aca
        elif tipo_ir == 'STK':
            if ganho > 0:
                if volume <= 35000: ir_devido = 0
                else: ir_devido = round(ganho * 0.15, 4)
            else: ir_devido = 0
            saldo_ir = 0
#        print(anomes, tipo_ir, ganho, volume, irrf, ir_devido, round(saldo_ir,4),sep='\t')        
        try:
           c.execute(c_ir_apuracao_ins, (None, anomes, tipo_ir, ganho, volume, irrf, ir_devido, round(saldo_ir,4)))
           conn.commit()
        except sqlite3.Error as err:
           conn.rollback()
           print('SQL ERROR: - Erro {!r} encontrado, errno e {}'.format(err, err.args[0]) + '\n')
           print(''.join('{} {} {} {} {} {}'.format(anomes, tipo_ir, ganho, volume, irrf, ir_devido, saldo_ir))+'\n')

    conn.close()
    time_end = time.time()
    print(time_end - time_start)
    return None

if __name__ == "__main__":
  main(sys.argv[1:]);