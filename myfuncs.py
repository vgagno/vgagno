import math

def truncate(number, digits) -> float:
    stepper = pow(10.0, digits)
    return math.trunc(stepper * number) / stepper

def myfloat(source):
    if source in ('', ' '): source = '0.0'
    elif source.find("."): source = source.replace(".", "")
    return float(source.replace(",", "."))

def myint(source):
    if source in ('', ' '): source = '0.0'
    elif source.find("."): source = source.replace(".", "")
    return int(source.replace(",", "."))


class corretora:
    def __init__(self, nome):
        self.razaosocial = nome
        self.taxa = 0

    def corretagem(self,taxa):
        self.taxa = taxa


class ValorMobiliario:
    def __init__(self, ticker):
        self.ticker = ticker
        self.qty = 0
        self.vunit = 0

    def mercado(ticker):
        if ticker.endswith('B'):
            liquidacao = truncate(vneg * 0.00006, 2)
            emolumentos = truncate(vneg * 0.00068, 2)
        else:
            liquidacao = truncate(vneg * 0.000275, 2)
            emolumentos = truncate(vneg * 0.00005, 2)
        return liquidacao, emolumentos

    def compra(self, ordem, data, qty, vunit, corretora, motivo):
        self.qty += qty
        vcustodia = self.qty * self.vunit
        vneg = qty * vunit
        liquidacao, emolumentos = mercado(ticker)
        vfinal = vneg + corretora.taxa + liquidacao + emolumentos
        self.vunit = (vfinal + vcustodia) / self.qty

