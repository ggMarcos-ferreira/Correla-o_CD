import queue
import threading
import math
from typing import List
from statistics import mean

dados_info = {
    "Nat2006us.dat": {"ano": 2006, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
    "Nat2007us.dat": {"ano": 2007, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
    "Nat2008us.dat": {"ano": 2008, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
    "Nat2009usPub.r20131202": {"ano": 2009, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
    "Nat2010PublicUS.r20131202": {"ano": 2010, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
    "Nat2011PublicUS.r20131211": {"ano": 2011, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
    "Nat2012PublicUS.r20131217": {"ano": 2012, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
    "Nat2013PublicUS.r20141016": {"ano": 2013, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
    "Nat2014PublicUS.c20150514.r20151022.txt": {"ano": 2014, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},
    "Nat2015PublicUS.c20160517.r20160907.txt": {"ano": 2015, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},
    "Nat2016PublicUS.c20170517.r20190620.txt": {"ano": 2016, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},
    "Nat2017PublicUS.c20180516.r20180808.txt": {"ano": 2017, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},
    "Nat2018PublicUS.c20190509.r20190717.txt": {"ano": 2018, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},
    "Nat2019PublicUS.c20200506.r20200915.txt": {"ano": 2019, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},
    "Nat2020PublicUS.c20210506.r20210812.txt": {"ano": 2020, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},
    "Nat2021US.txt": {"ano": 2021, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},
}

fila_dados = queue.Queue()

for dado in dados_info:
    fila_dados.put(dado)

def cria_threads(num_threads, fila_dados):
    for i in range(num_threads):
        t = threading.Thread(target=processa_dados, args=(fila_dados,))
        t.start()

def processa_dados(fila_dados):
    while True:
        try:
            dado = fila_dados.get(block=False)
            processa_dado(dado)
        except queue.Empty:
            break

def processa_dado(dado):
    idades_mae = []
    pesos_bebe = []
  
    if dado not in dados_info:
        raise ValueError("Dado não suportado")
        
    info = dados_info[dado] 
    ano = info["ano"]
    idade_pos = info["idade_pos"]
    peso_pos = info["peso_pos"]
    
    idade_peso_dict = {}
    with open(dado, 'r') as f:
        for linha in f:
            idade = int(linha[idade_pos])
            peso = int(linha[peso_pos])

            if idade not in idade_peso_dict:
                idade_peso_dict[idade] = []
            idade_peso_dict[idade].append(peso)

        for idade, pesos in idade_peso_dict.items():
            if 13 <= idade <= 19: 
                media_peso = mean(pesos)
                idades_mae.append(idade)
                pesos_bebe.append(media_peso)

    correlacao_total = correlacao(idades_mae, pesos_bebe)
    if dado == "Nat2021US.txt":
        print("CORRELAÇÃO:\n")
        print(f"Correlação Total do Dados= {correlacao_total:.2f} Média de Peso= {media_peso:.2f}g")


def variancia(idades_mae: List[float]) -> float:
    assert len(idades_mae) >= 2
    n = len(idades_mae)
    desvios = de_media(idades_mae)
    return sum_of_squares(desvios) / (n - 1)

def desvio_padrao(idades_mae: List[float]) -> float:
    return math.sqrt(variancia(idades_mae))

def covariancia(idades_mae: List[float], pesos_bebe: List[float]) -> float:
    assert len(idades_mae) == len(pesos_bebe)
    return dot(de_media(idades_mae), de_media(pesos_bebe)) / (len(idades_mae) - 1)

Vector = List[float]

def dot(v: Vector, w: Vector) -> float:
    assert len(v) == len(w)
    return sum(v_i * w_i for v_i, w_i in zip(v, w))

def sum_of_squares(v: Vector) -> float:
    return dot(v, v)

def de_media(idades_mae: List[float]) -> List[float]:
    x_bar = mean(idades_mae)
    return [x - x_bar for x in idades_mae]

def correlacao(idades_mae: List[float], pesos_bebe: List[float]) -> float:
    stdev_x = desvio_padrao(idades_mae)
    stdev_y = desvio_padrao(pesos_bebe)
    
    if stdev_x > 0 and stdev_y > 0:
        return covariancia(idades_mae, pesos_bebe) / stdev_x / stdev_y
    else:
        return 0

cria_threads(2, fila_dados)