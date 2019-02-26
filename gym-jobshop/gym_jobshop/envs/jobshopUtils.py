import pandas as pd
import numpy as np
from enum import Enum

class rules(Enum):
    RANDOM = 0
    FIFO = 1
    LIFO = 2
    SOT = 3
    LOT = 4
    SROT = 5
    LROT = 6
    LRO = 7
    MRO = 8
    DD = 9
    SS = 10
    DS = 11
    SSROT = 12
    DSROT = 13
    SSRO = 14
    DSRO = 15



class Machine():
    def __init__(self, id):
        self.idMachine = id
        self.processingJob = -1    # id of job processed
        # queueItem_columns = ['IdPedido','FechaPedido','FechaEntrega','FechaCola','TiempoProcesamiento','TiempoOcupacion','TiempoRestante','n_pasos','n_pasos_restantes'] # 'ss','ds','ssrot','dsrot','ssro','dsro'
        # self.queue = pd.DataFrame(columns=queueItem_columns)     # empty panda object of type queueItem
    def selectJob(self,idRule, queue):
        chosen = []
        # todo: compute dynamic attributes
        if idRule == rules.RANDOM.value:
            chosen = queue.sample(n=1)
        elif idRule == rules.FIFO.value:
            chosen = pd.DataFrame([queue.sort_values(by=['queueDate']).iloc[0]])
        elif idRule == rules.LIFO.value:
            chosen = pd.DataFrame([queue.sort_values(by=['queueDate']).iloc[-1]])
        elif idRule == rules.SOT.value:
            chosen = pd.DataFrame([queue.sort_values(by=['operationTime']).iloc[0]])
        elif idRule == rules.LOT.value:
            chosen = pd.DataFrame([queue.sort_values(by=['operationTime']).iloc[-1]])
        elif idRule == rules.SROT.value:
            pass
        elif idRule == rules.LROT.value:
            pass
        elif idRule == rules.LRO.value:
            chosen = pd.DataFrame([queue.sort_values(by=['remainingSteps']).iloc[0]])
        elif idRule == rules.MRO.value:
            chosen = pd.DataFrame([queue.sort_values(by=['remainingSteps']).iloc[-1]])
        elif idRule == rules.DD.value:
            chosen = pd.DataFrame([queue.sort_values(by=['deliverDate']).iloc[0]])
        elif idRule == rules.SS.value:
            chosen = pd.DataFrame([queue.sort_values(by=['ss']).iloc[0]])
        elif idRule == rules.DS.value:
            chosen = pd.DataFrame([queue.sort_values(by=['ds']).iloc[0]])
        elif idRule == rules.SSROT.value:
            chosen = pd.DataFrame([queue.sort_values(by=['ssrot']).iloc[0]])
        elif idRule == rules.DSROT.value:
            chosen = pd.DataFrame([queue.sort_values(by=['dsrot']).iloc[0]])
        elif idRule == rules.SSRO.value:
            chosen = pd.DataFrame([queue.sort_values(by=['ssro']).iloc[0]])
        elif idRule == rules.DSRO.value:
            chosen = pd.DataFrame([self.sort_values(by=['dsro']).iloc[0]])
        # self.processingJob = chosen.copy(deep=True)
        queue.drop(chosen.index, inplace=True)
        self.processingJob = chosen['IdPedido'].values[0]

        return self.processingJob

    def updateQueueTimes(self):
        pass
