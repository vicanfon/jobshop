import pandas as pd
import numpy as np

# event structure: IdPedido TEvent Fase CodMaquina event executed

class eventSimulator():
    def __init__(self, Orders, Routes):
        # Initialize list of events
        self.Routes = Routes
        self.df_Events = Orders.join(Routes.set_index('CodPieza'), on='CodPieza').query('Fase == 10')[
            ['IdPedido', 'FechaPedido', 'Fase']].copy(deep=True)
        self.df_Events['indexEvent'] = self.df_Events['IdPedido'].astype(str) + "_" + self.df_Events['Fase'].astype(str)
        self.df_Events['event'] = 1
        self.df_Events['executed'] = False
        self.df_Events = self.df_Events.set_index('indexEvent').rename(columns={'FechaPedido': 'TEvent'})
        self.df_Events.loc[:, 'TEvent'] = pd.to_datetime(self.df_Events.loc[:, 'TEvent'])

        self.clock = 0

    def createEvents(self, pedidos, eventtype, clock):
        # pedidos: 'IdPedido', 'FechaPedido', 'Fase','CodMaquina'
        pedidos = pedidos[['IdPedido', 'Fase']]
        pedidos['indexEvent'] = pedidos['IdPedido'].astype(str) + "_" + pedidos['Fase'].astype(str)
        pedidos['event'] = eventtype
        pedidos['executed'] = False
        pedidos['TEvent'] = clock
        return pedidos.set_index('indexEvent')

    def processEvents(self, events):
        self.df_Events.loc[events.index, 'executed'] = True

    def addEvents(self, events):
        # add new events and mark some to executed
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