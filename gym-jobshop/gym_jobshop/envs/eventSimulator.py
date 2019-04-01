import pandas as pd
import numpy as np

# event structure: IdPedido TEvent Fase CodMaquina event executed

class eventSimulator():
    def __init__(self, Orders, Routes):
        # Initialize list of events
        l2 = pd.DataFrame([1, 2, 3], columns=['event'])
        l2['key'] = 0
        Routes['key'] = 0
        Routes['n_pasos'] = Routes.groupby('CodPieza')['CodPieza'].transform('count')
        Routes['n_pasos_restantes'] = Routes['n_pasos']
        Routes['TTPreparacion'] = Routes.groupby('CodPieza')['TPreparacion'].transform('sum')
        Routes['TTUnitario'] = Routes.groupby('CodPieza')['TUnitario'].transform('sum')
        Orders = Orders.astype({'IdPedido': 'int64'})

        Events = Orders.join(Routes.set_index('CodPieza'), on='CodPieza').merge(l2, how='outer', on='key').drop('key',
                                                                                                                axis=1)
        # Events = Events.merge(l2,how='outer',on='key').drop('key',axis=1)
        Events['indexEvent'] = Events['IdPedido'].astype(str) + "_" + Events['Fase'].astype(str)
        Events['executed'] = False
        Events = Events.set_index('indexEvent').rename(columns={'FechaPedido': 'TEvent'})
        Events.loc[:, 'TEvent'] = pd.to_datetime(Events.loc[:, 'TEvent'])
        Events['TiempoProcesamiento'] = Events['TPreparacion'] + Events['TUnitario'] * \
                                        Events['Lote']
        Events['TiempoOcupacion'] = Events['TTPreparacion'] + Events['TTUnitario'] * \
                                    Events['Lote']
        Events['TiempoRestante'] = Events['TiempoOcupacion']
        Events['FechaCola'] = pd.Timestamp('1978-01-01')

        self.clock = pd.Timestamp('1978-01-01')

    def createEvents(self, pedidos, eventtype, clock):
        # pedidos: 'IdPedido', 'FechaPedido', 'Fase','CodMaquina'
        #if len(pedidos)>0:
            # pedidos = pedidos[['IdPedido', 'Fase']]
            # pedidos = pedidos[['IdPedido', 'Fase', 'event','executed','TEvent']] todo: borrar esta
        pedidos['indexEvent'] = pedidos['IdPedido'].astype(str) + "_" + pedidos['Fase'].astype(str)
        pedidos['event'] = eventtype
        pedidos['executed'] = False
        pedidos['TEvent'] = clock
        return pedidos.set_index('indexEvent')

    def processEvents(self, events):
        self.df_Events.loc[events.index, 'executed'] = True

    def addEvents(self, events):
        # add new events and mark some to executed
        # self.df_Events = self.df_Events.append(events[['IdPedido', 'TEvent', 'Fase', 'event','executed']])
        self.df_Events = self.df_Events.append(events)

    def nextEvents(self):
        eventsNonProcessed=self.df_Events[self.df_Events['executed'] == False].sort_values(by=['TEvent', 'Fase'])
        if len(eventsNonProcessed)>0:
            self.clock=eventsNonProcessed.iloc[0]['TEvent']
            # print("clock: " +str(self.clock))
            result = eventsNonProcessed[eventsNonProcessed["TEvent"] == self.clock].copy(deep=True)
        else:
            result=[]

        return self.clock, result
    
    def history(self):
        return self.df_Events.copy(deep=True)