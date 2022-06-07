from tabula import read_pdf
import json
import datetime
from myfuncs import *
import sys


def nota_xp(pdf_path, page):
### area = [top, left, bottom, right]
### Tabela Nota, Folhas e Data
    area_nota_data = [[55.86, 431.27, 67.0, 560.0]]
    parsed = read_pdf(pdf_path, output_format="json", pages=page, multiple_tables=False, area=area_nota_data, stream=True, encoding="utf-8")
#    print(json.dumps(parsed, indent=4, sort_keys=True), len(parsed[0]['data']))
    nota = parsed[0]['data'][1][0]['text'] # Nota
    folha = parsed[0]['data'][1][2]['text'] # Nota
    data = parsed[0]['data'][1][3]['text'] # Data

### Tabela Bolsa
### area = [top, left, bottom, right]
    sequencia = True
    pagina_atual = page
    negocios = []

    while sequencia:
        area_bolsa = [[515.0, 300.0, 555.0, 556.0]]
        parsed = read_pdf(pdf_path, output_format="json", pages=pagina_atual, multiple_tables=False, area=area_bolsa, stream=True, encoding="utf-8")

        if  parsed[0]['data'][2][1]['text'] == "CONTINUA...": tem_mais_paginas = True
        else: tem_mais_paginas = False

        area_negociacao = [[244.0, 43.0, 440.73, 580.0]]
        parsed = read_pdf(pdf_path, output_format="json", pages=pagina_atual, multiple_tables=False, area=area_negociacao, stream=True, encoding="utf-8")
        print(pagina_atual)
#        print(json.dumps(parsed, indent=4, sort_keys=True), len(parsed[0]['data']))
        for i in range(0, len(parsed[0]['data'])):
            negocio = []
            for j in range(0, len(parsed[0]['data'][i])):
#                print(i, j, parsed[0]['data'][i][j]['text'])
                if parsed[0]['data'][i][j]['text'] not in ("", "#"): negocio.append(parsed[0]['data'][i][j]['text'])
            negocios.append(negocio)
#            print(i, negocio)

        if tem_mais_paginas:
            pagina_atual = pagina_atual + 1
            continue

        area_bolsa = [[515.0, 300.0, 555.0, 556.0]]
        parsed = read_pdf(pdf_path, output_format="json", pages=pagina_atual, multiple_tables=False, area=area_bolsa, stream=True, encoding="utf-8")
        emolumentos = parsed[0]['data'][2][1]['text']
        sequencia = False
#        print(json.dumps(parsed, indent=4, sort_keys=True), len(parsed[0]['data']), parsed[0]['data'][2][1]['text'])

### Tabela Clearing
        area_clearing = [[467.0, 300.0, 510.0, 556.0]]
        parsed = read_pdf(pdf_path, output_format="json", pages=pagina_atual, multiple_tables=False, area=area_clearing, stream=True, encoding="utf-8")
#    print(json.dumps(parsed, indent=4, sort_keys=True), len(parsed[0]['data']))
        liquidacao = parsed[0]['data'][1][1]['text']
        registro = parsed[0]['data'][2][1]['text']

### Tabela Corretagem / Despesa
        area_corretagem = [[574.0, 300.0, 640.0, 556.0]]
        parsed = read_pdf(pdf_path, output_format="json", pages=pagina_atual, multiple_tables=False, area=area_corretagem, stream=True, encoding="utf-8")
#    print(json.dumps(parsed, indent=4, sort_keys=True), len(parsed[0]['data'])) 
        corretagem = parsed[0]['data'][0][1]['text']
        iss = parsed[0]['data'][3][1]['text']
        irrf = parsed[0]['data'][4][1]['text']

### Tabela Resumo dos Negocios    
        area_resumo = [[457.0, 25.0, 555.0, 295.0]]
        parsed = read_pdf(pdf_path, output_format="json", pages=pagina_atual, multiple_tables=False, area=area_resumo, stream=True, encoding="utf-8")
#    print(json.dumps(parsed, indent=4, sort_keys=True), len(parsed[0]['data']))
        vista_c = parsed[0]['data'][2][1]['text']
        vista_v = parsed[0]['data'][1][1]['text']
        opcao_c = parsed[0]['data'][3][1]['text']
        opcao_v = parsed[0]['data'][4][1]['text']
        voper = parsed[0]['data'][7][1]['text']

    resultado = { 'nota': nota, 
                  'data': datetime.datetime.strptime(data, '%d/%m/%Y'),
                  'negocios': negocios,
                  'liquidacao': myfloat(liquidacao),
                  'registro': myfloat(registro),
                  'emolumentos': myfloat(emolumentos),
                  'corretagem': myfloat(corretagem),
                  'iss': myfloat(iss),
                  'irrf': myfloat(irrf),
                  'vista': [myfloat(vista_c), myfloat(vista_v)],
                  'opcao': [myfloat(opcao_c), myfloat(opcao_v)],
                  'voper': myfloat(voper)
                }
    return resultado

def nota_orama(pdf_path, page):
### area = [top, left, bottom, right]
### Tabela Nota, Folhas e Data
    area_nota_data = [[43.36, 455.23, 63.36, 570]]
    parsed = read_pdf(pdf_path, output_format="json", pages=page, multiple_tables=False, area=area_nota_data, stream=True, encoding="utf-8")
    nota = parsed[0]['data'][1][0]['text'] # Nota
    folha = parsed[0]['data'][1][1]['text'] # Nota
    data = parsed[0]['data'][1][2]['text'] # Data 

    pag_atual = int(folha[0:2])
    max_pag = int(folha[folha.find(' de ')+4:])

### area = [top, left, bottom, right]
    negocios = []
    for z in range(pag_atual, max_pag + 1):
#        print(z, max_pag)
        area_negociacao = [[221.73, 48.4, 470.73, 572.15]]
        parsed = read_pdf(pdf_path, output_format="json", pages=z, multiple_tables=False, area=area_negociacao, stream=True, encoding="utf-8")
#        print(parsed)
        for i in range(0, len(parsed[0]['data'])):
            negocio = []
            for j in range(0, len(parsed[0]['data'][i])):
#                print(i, j, parsed[0]['data'][i][j]['text'])
                if parsed[0]['data'][i][j]['text'] not in ("ON", "PN"): negocio.append(parsed[0]['data'][i][j]['text'])
#            print(i, negocio)
            negocios.append(negocio)

### Tabela Clearing
    area_clearing = [[495.01, 310.03, 529.0, 575.0]]
    parsed = read_pdf(pdf_path, output_format="json", pages=max_pag, multiple_tables=False, area=area_clearing, stream=True, encoding="utf-8")
    liquidacao = parsed[0]['data'][1][1]['text']
    registro = parsed[0]['data'][2][1]['text']

### Tabela Bolsa
    area_bolsa = [[531.0, 310.03, 580.0, 575.0]]
    parsed = read_pdf(pdf_path, output_format="json", pages=max_pag, multiple_tables=False, area=area_bolsa, stream=True, encoding="utf-8")
    emolumentos = parsed[0]['data'][3][1]['text']

### Tabela Corretagem / Despesa
    area_corretagem = [[590.0, 310.03, 665.0, 575.0]]
    parsed = read_pdf(pdf_path, output_format="json", pages=max_pag, multiple_tables=False, area=area_corretagem, stream=True, encoding="utf-8")
#    print(json.dumps(parsed, indent=4, sort_keys=True), len(parsed[0]['data']))
    corretagem = parsed[0]['data'][0][1]['text']
    iss = parsed[0]['data'][3][1]['text']
    irrf = parsed[0]['data'][4][1]['text']

### Tabela Resumo dos Negocios    
    area_resumo = [[486.0, 37.94, 578.0, 293.0]]
    parsed = read_pdf(pdf_path, output_format="json", pages=max_pag, multiple_tables=False, area=area_resumo, stream=True, encoding="utf-8")
#    print(json.dumps(parsed, indent=4, sort_keys=True), len(parsed[0]['data']))
    vista_c = parsed[0]['data'][1][1]['text']
    vista_v = parsed[0]['data'][0][1]['text']
    opcao_c = parsed[0]['data'][2][1]['text']
    opcao_v = parsed[0]['data'][3][1]['text']
    voper = parsed[0]['data'][9][1]['text']

    resultado = { 'nota': nota, 
                  'data': datetime.datetime.strptime(data, '%d/%m/%Y'),
                  'negocios': negocios,
                  'liquidacao': myfloat(liquidacao),
                  'registro': myfloat(registro),
                  'emolumentos': myfloat(emolumentos),
                  'corretagem': myfloat(corretagem),
                  'iss': myfloat(iss),
                  'irrf': myfloat(irrf),
                  'vista': [myfloat(vista_c), myfloat(vista_v)],
                  'opcao': [myfloat(opcao_c), myfloat(opcao_v)],
                  'voper': myfloat(voper)
                }
    return resultado


# {'nota': '325', 'data': '05/05/2022', 'negocios': [['BOVESPA 1', 'V', 'OPCAO DE VENDA', '05/22', 'VBBRQ198', 'ON', 'NM VIBRA', '500', '0,47', '235,00', 'C'], 
# ['BOVESPA 1', 'V', 'OPCAO DE VENDA', '05/22', 'WEGEQ275', 'ON', 'NM WEG', '500', '0,55', '275,00', 'C']], 
# 'liquidacao': '0,14', 'registro': '0,35', 'emolumentos': '0,18', 'corretagem': '2,00', 'iss': '0,04', 'irrf': '0,02'}

def calculo_resultado(re):
    ativos = {}

# ['1-BOVESPA', 'C', 'VISTA', 'FII BC FUND', 'BRCR11', 'CI ER', '1', '67,93', '67,93', 'D'] - XP
# ['BOVESPA 1', 'V', 'OPCAO DE COMPRA', '06/22', 'BBSEF255', 'ON', 'NM BBSEGURIDADE', '100', '0,82', '82,00', 'C'] - ORAMA
    
    for i in range(0, len(re['negocios'])):
        ticker = re['negocios'][i][4]
        vtotal = myfloat(re['negocios'][i][8])
        vprop = vtotal/re['voper']
        if ticker not in ativos.keys():
            ativos[ticker] = { 'qty': myint(re['negocios'][i][6]), 
                               'vunit': myfloat(re['negocios'][i][7]),
                               'vtotal': vtotal,
                               'tipo': re['negocios'][i][1],
                               'liquidacao': vprop*re['liquidacao'],
                               'registro': vprop*re['registro'],
                               'emolumentos': vprop*re['emolumentos'],
                               'corretagem': (re['corretagem']+re['iss'])/len(re['negocios'])
                            }
#            print(ativos[re['negocios'][i][4]])
#            ativos[ticker]['custos'] = ativos[ticker]['liquidacao'] + ativos[ticker]['registro'] + ativos[ticker]['emolumentos'] + ativos[ticker]['corretagem']
        else:
            ativos[ticker] = { 'qty': ativos[ticker]['qty'] + myint(re['negocios'][i][6]),
                               'vunit': ((ativos[ticker]['vunit'] * ativos[ticker]['qty']) + \
                                        (myfloat(re['negocios'][i][7]) * myint(re['negocios'][i][6]))) / \
                                        (ativos[ticker]['qty'] + myint(re['negocios'][i][6])),
                               'tipo': re['negocios'][i][1],
                               'vtotal': ativos[ticker]['vtotal'] + vtotal,
                               'liquidacao': ativos[ticker]['liquidacao'] + vprop*re['liquidacao'],
                               'registro': ativos[ticker]['registro'] + vprop*re['registro'],
                               'emolumentos': ativos[ticker]['emolumentos'] + vprop*re['emolumentos'],
                               'corretagem': ativos[ticker]['corretagem'] + (re['corretagem']+re['iss'])/len(re['negocios'])
                             }
        ativos[ticker]['custos'] = ativos[ticker]['liquidacao'] + ativos[ticker]['registro'] + ativos[ticker]['emolumentos'] + ativos[ticker]['corretagem']
        
    for ticker in ativos.keys():
        if ativos[ticker]['tipo'] == 'V':
            if re['vista'][1] != 0: ativos[ticker]['irrf'] = truncate(re['vista'][1] * 0.00005, 2) * re['vista'][1] / ativos[ticker]['vtotal']
            elif re['opcao'][1] != 0: ativos[ticker]['irrf'] = truncate((re['opcao'][1] - re['opcao'][0]) * 0.00005, 2) * re['opcao'][1] / ativos[ticker]['vtotal']
        else: ativos[ticker]['irrf'] = 0
            
    return ativos

def extract_information(pdf_path, page):
### area = [top, left, bottom, right]

    parsed = read_pdf(pdf_path, output_format="json", pages=page, area=[[0.0, 0.0, 214.0, 243.0]], multiple_tables=False, stream=True, encoding="utf-8")

#    print(parsed[0]['data'][0][0]['text'])

    if parsed[0]['data'][1][1]['text'].startswith('ORAMA'): resultado = nota_orama(pdf_path, page)
    elif parsed[0]['data'][1][1]['text'].startswith('XP'): resultado = nota_xp(pdf_path, page)

    ativos = calculo_resultado(resultado)
    
    for ticker in ativos.keys(): print(ticker, ': \n', ativos[ticker])
    

### Geração PDF corretora nova
#    print(json.dumps(parsed, indent=4, sort_keys=True))
### Apenas trechos relevantes (dados no campo text)
#    for st in range(len(parsed)):
#        for nd in range(len(parsed[st]['data'])):
#            for rd in range(len(parsed[st]['data'][nd])):
#                if parsed[st]['data'][nd][rd]['text'] != '': print('First: ', st, 'Second: ', nd, 'Third: ', rd, 'Text: ', parsed[st]['data'][nd][rd]['text'])

    return parsed

if __name__ == '__main__':
#    path = r'D:\vgagno\Dropbox\PersonalFiles\Dados Financeiros\NotasCorretagem\Necton\2021\20211117-VIVT3.pdf'
#    path = r'D:\vgagno\Dropbox\PersonalFiles\Dados Financeiros\NotasCorretagem\Orama\2022\20220519-VBBRQ198-VBBRR193-VALEF883-BOVAF107.pdf'
#    path = '20220509-BRCR11-CPFF11.pdf'
#    main(sys.argv[1:]);
#    extract_information(path)
#    print(sys.argv[1])
    if len(sys.argv) < 2 or 'help' in sys.argv:
        print('Uso:')
        print('\tpdfextract.py <Arquivo> [<Página>]')
        print('\t<Arquivo>: Arquivo com a nota de corretagem')
        print('\t[<Página>]: Número da página (opcional) Default = 1')
        print(len(sys.argv),'\t:\t',sys.argv)
        sys.exit(2)
    
    path = sys.argv[1]
    try: page = sys.argv[2]
    except IndexError as err: page = 1
    extract_information(path, page)