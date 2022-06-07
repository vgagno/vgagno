import sqlite3
import time
import os
import json
import re
import base64
from datetime import date, datetime
from urllib.request import Request, urlopen, urlretrieve
import pandas as pd
import xml.etree.ElementTree as ET
import sys


def url_getc(url):

    # Opens a website and read its
    # binary contents (HTTP Response Body)

    #making request to the website
    req = Request(url, headers={'User-Agent':'Mozzila/5.0'})
    response = urlopen(req)

    #reading contents of the website
    return response.read()

def get_filename_from_cd(cd):
    """
    Get filename from content-disposition
    """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None
    return fname[0][1:-1]



def main(argv):


    if len(argv) < 1 or 'help' in argv:
        print('Uso:')
        print('\t3-GeraCustodiaIR.py <Database>')
        print('\t<Database>: Arquivo de banco a ser usado')
        print(len(argv),'\t:\t',argv)
        sys.exit(2)

    time_start = time.time()
    print('Opening database ',argv[0])
    conn = sqlite3.connect(argv[0])
    c = conn.cursor()

#    c_fiidesc_qry = "SELECT d.ticker, d.cnpj, e.data_mov, s.data_mov FROM fii_descricao AS d, \
#                     (SELECT MIN(data_mov) from ORDENS WHERE operacao = 'C' AND ticker = d.ticker) AS e, \
#                     (SELECT MAX(data_mov) from ORDENS WHERE operacao = 'V' AND ticker = d.ticker) AS s \
#                     WHERE ativo = 1"

    c_fiidesc_qry = "SELECT id, ticker, cnpj FROM fii_descricao WHERE ativo = 1 order by ticker"
    c_fnet_ins = "INSERT INTO fnet_read (doc_id, doc_url, ticker, administrador, cnpj_admin, tipo, dt_aprov, dt_com, dt_pag, rend_unit, mesref, ano, anomes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    url_list_docs = 'https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados?d={}&s={}&l={}&o[0][dataEntrega]=desc&idCategoriaDocumento={}&situacao={}&cnpj={}'
    url_get_data = 'https://fnet.bmfbovespa.com.br/fnet/publico/downloadDocumento?id={}'
    #url_get_data = 'https://fnet.bmfbovespa.com.br/fnet/publico/exibirDocumento?id={}&cvm=true&'
    # https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo=30130708000128&idCategoriaDocumento=14
# https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo=30130708000128&idCategoriaDocumento=7&situacao=A
# https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados?d=1&s=0&l=1&o[0][dataEntrega]=desc&idCategoriaDocumento=14&situacao=A&cnpj=11839593000109
    d='1'
    s='0'
    l='1'
    cat_rend = '14'
    cat_rger = '7'
    situacao='A'

#    conn = sqlite3.connect('investimentos.db')
#    c = conn.cursor()
    #print("Updating values.... ", end=' ', flush=True)

    url_tickers = {}
    url_docs = {}
    json_parsed = {}

    try:
        c.execute(c_fiidesc_qry)
    except:
        print('Erro ao abrir o arquivo ',argv[0])
        print(len(argv),'\t:\t',argv)
        sys.exit(2)  

    rows = c.fetchall()
    for fii_row in rows:
        fii_id = fii_row[0]
        ticker = fii_row[1]
        cnpj = str(fii_row[2])
        url_tickers[ticker] = url_list_docs.format(d, s, l, cat_rend, situacao, cnpj)
        req = Request(url_tickers[ticker], headers={'User-Agent':'Mozzila/5.0'})
        xhtml = urlopen(req).read().decode('utf-8')
#        print('Reading URL: ', url_tickers[ticker], '\t Ticker: \t', ticker)
        json_parsed[ticker] = json.loads(xhtml)
        for doc in json_parsed[ticker]['data']:
            id_doc = str(doc['id'])
#            print(url_get_data.format(id_doc))
            url_docs[ticker] = url_get_data.format(id_doc)
            req = Request(url_docs[ticker], headers={'User-Agent':'Mozilla/5.0'})
            xhtml = urlopen(req).read().decode('utf-8')
            xmld = ET.fromstring(base64.b64decode(xhtml))
            DadosGerais = xmld[0]
            Rendimento = xmld[1][0]
            try:
                tipo = 'R'
                DtAprovacao = Rendimento[0]
                DtBase = Rendimento[1]
                DtPag = Rendimento[2]
                VlrProv = Rendimento[3]
                Ref = Rendimento[4]
                Ano = Rendimento[5]
            except IndexError as err:
                Amortizacao = xmld[1][1]
                tipo = 'A'
                DtAprovacao = Amortizacao[0]
                DtBase = Amortizacao[1]
                DtPag = Amortizacao[2]
                VlrProv = Amortizacao[3]
                Ref = Amortizacao[4]
                Ano = Amortizacao[5]
             
            anomes = datetime.date(datetime.strptime(DtPag.text, '%Y-%m-%d')).strftime('%Y%m')
          
            try:
                c.execute(c_fnet_ins, (id_doc, url_docs[ticker], DadosGerais[7].text, DadosGerais[2].text, DadosGerais[3].text, tipo, DtAprovacao.text, DtBase.text, DtPag.text,   VlrProv.text, Ref.text, Ano.text, anomes))
                conn.commit()
            except sqlite3.IntegrityError:
                print("Arquivo {} já carregado!\t Ticker: {}\t Data-Ex:{}\t Data pag.:{}".format(id_doc, ticker, DtBase.text, DtPag.text))
            except sqlite3.Error as err:
                conn.rollback()
                print('SQL ERROR: - Erro {!r} encontrado, errno e {}'.format(err, err.args[0]) + '\n')
                print(''.join('{} {} {} {} {} {} {} {} {} {} {} {} {}'.format(id_doc, url_docs[ticker], DadosGerais[7].text, DadosGerais[2].text, DadosGerais[3].text, tipo, DtAprovacao.text, DtBase.text, DtPag.text, VlrProv.text, Ref.text, Ano.text, anomes))+'\n')
            else:
                print('Reading URL: ', url_tickers[ticker], '\t Ticker: \t', ticker)
                print(url_get_data.format(id_doc))

#    c_fnet_ins = "INSERT INTO fnet_read (doc_id, doc_url, ticker, administrador, cnpj_admin, dt_aprov, dt_com, dt_pag, rend_unit, mes_ref, ano, anomes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

       
#          print(DadosGerais[7].tag, DadosGerais[7].text)
#          print(DadosGerais[2].tag, DadosGerais[2].text)
#          print(DadosGerais[3].tag, DadosGerais[3].text)
#          print(DtAprovacao.tag, DtAprovacao.text)
#          print(DtBase.tag, DtBase.text)
#          print(DtPag.tag, DtPag.text)
#          print(VlrProv.tag, VlrProv.text)
#          print(Ref.tag, Ref.text)
#          print(Ano.tag, Ano.text)
#          os.system("pause")
    return None

if __name__ == "__main__":
    main(sys.argv[1:]);


# <DadosEconomicoFinanceiros xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
	# <DadosGerais>
		# <NomeFundo>FII BTGP LOGISTICA</NomeFundo>
		# <CNPJFundo>11839593000109</CNPJFundo>
		# <NomeAdministrador>BTG Pactual Serviços Financeiros S.A. DTVM</NomeAdministrador>
		# <CNPJAdministrador>59281253000123</CNPJAdministrador>
		# <ResponsavelInformacao>Lucas Massola</ResponsavelInformacao>
		# <TelefoneContato>(11) 3383-2513</TelefoneContato>
		# <CodISINCota>BRBTLGCTF000</CodISINCota>
		# <CodNegociacaoCota>BTLG11</CodNegociacaoCota>
	# </DadosGerais>
	# <InformeRendimentos>
		# <Rendimento>
			# <DataAprovacao>2021-12-15</DataAprovacao>
			# <DataBase>2021-12-15</DataBase>
			# <DataPagamento>2021-12-23</DataPagamento>
			# <ValorProventoCota>0.72</ValorProventoCota>
			# <PeriodoReferencia>Novembro</PeriodoReferencia>
			# <Ano>2021</Ano>
			# <RendimentoIsentoIR>true</RendimentoIsentoIR>
		# </Rendimento>
		# <Amortizacao tipo=""/>
	# </InformeRendimentos>
# </DadosEconomicoFinanceiros>








# https://fnet.bmfbovespa.com.br/fnet/publico/pesquisarGerenciadorDocumentosDados?d=1&s=0&l=100&idCategoriaDocumento=14&situacao=A&cnpj=11839593000109
# https://fnet.bmfbovespa.com.br/fnet/publico/visualizarDocumento?id=247031&cvm=true
# https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM?cnpjFundo=11839593000109&idCategoriaDocumento=14

#print(url_tickers)

#try:
#   req = Request(url, headers={'User-Agent':'Mozzila/5.0'})
#   response = urlopen(req)
#   html = response.read()
##   print (html)
#except HTTPError as e:
#   print(e.status, e.reason)
#except URLError as e:
#   print(e.reason)

#soup = BeautifulSoup(html,'html.parser')
#print(soup)





#data_cell = soup.find_all('td')

# n_fiis=len(data_cell)/26
# n_fiis=int(n_fiis)
# fii=[]
# lista_fiis=[]

# for i in range(n_fiis):
# #   print('i=', i)
   # for j in range(26):
# #      print('j=', j)
      # fii.append(data_cell[(26*i)+j].get_text())
   # lista_fiis.append(fii)
# #   print(fii[0],fii[1],fii[2],fii[3],fii[4],fii[5],fii[6],fii[7],fii[8],fii[9],fii[10],fii[11],fii[12],fii[13],fii[14],fii[15],fii[16],fii[17],fii[18],fii[19],fii[20],fii[21])
# #   print(fii[22],fii[23],fii[24],fii[25])
   # fii = []
   # print(lista_fiis[i])
# #   print('Tag Name: {}; Content:{}'.format(cell.name,cell.get_text())) 





