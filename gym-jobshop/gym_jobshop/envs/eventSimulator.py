import pandas as pd
import numpy as np

# event structure: IdPedido TEvent Fase CodMaquina event executed

class eventSimulator():
    def __init__(self, Orders, Routes):
        # Initialize list of events
        self.Routes = Routes
        self.df_Events = Orders.join(Routes.set_index('CodPieza'), on='CodPieza').query('Fase == 10')[
            ['IdPedido', 'FechaPedido', 'Fase','CodMaquina']].copy(deep=True)
        self.df_Events['indexEvent'] = self.df_Events['IdPedido'].astype(str) + "_" + self.df_Events['Fase'].astype(str)
        self.df_Events['event'] = 1
        self.df_Events['executed'] = False
        self.df_Events = self.df_Events.set_index('indexEvent').rename(columns={'FechaPedido': 'TEvent'})
        self.df_Events.loc[:, 'TEvent'] = pd.to_datetime(self.df_Events.loc[:, 'TEvent'])

        self.clock = 0


    def createEvents(self, pedidos):
        # pedidos: 'IdPedido', 'FechaPedido', 'Fase','CodMaquina'

        self.df_Events = pedidos.join(self.Routes.set_index('CodPieza'), on={'CodPieza','Lote'}).query('Fase == 10')[
            ['IdPedido', 'FechaPedido', 'Fase', 'CodMaquina']].copy(deep=True)
        self.df_Events['indexEvent'] = self.df_Events['IdPedido'].astype(str) + "_" + self.df_Events['Fase'].astype(str)
        self.df_Events['event'] = 1
        self.df_Events['executed'] = False
        self.df_Events = self.df_Events.set_index('indexEvent').rename(columns={'FechaPedido': 'TEvent'})
        self.df_Events.loc[:, 'TEvent'] = pd.to_datetime(self.df_Events.loc[:, 'TEvent'])

    def addEvent(self, events, event, clock):
        
        # add new events and mark some to executed
        if (event==1):
            self.df_Events.loc[events.index, 'executed'] = True
        elif (event == 2):
            # newEvent=self.df_Events[self.df_Events['IdPedido'] == events].sort_values(by=['TEvent', 'Fase']).query("executed==True").iloc[-1].copy(deep=True)
            newEvents2 = events.copy(deep=True)
            newEvents2[:,'event']=2
            newEvents2[:, 'TEvent']=pd.to_datetime(clock)
            self.df_Events = self.df_Events.append(newEvents2)
            # newEvent['selectedRule']=selectedRule
            newEvents3 = events.copy(deep=True)
            newEvents3[:, 'event'] = 3
            newEvents3[:, 'executed']=False
            newEvents3[:, 'TEvent'] = pd.to_datetime(clock)
            self.df_Events = self.df_Events.append(newEvents3)
            # TODO: round time with newEvent['TEvent'] = newEvent['TEvent'].round('s')
        elif (event==3):
            self.df_Events.loc[events.index, 'executed'] = True
            newEvents1 = events.copy(deep=True)
            newEvents1[:, 'event'] = 1
            newEvents1[:, 'Fase'] += 10
            newEvents1[:, 'executed'] = False
            # compare Fase with end and add only the needed
            self.df_Events = self.df_Events.append(newEvents1)

    def nextEvents(self):
        if len(self.df_Events[self.df_Events['executed']==False].sort_values(by=['TEvent','Fase']))>0:
            self.clock=self.df_Events[self.df_Events['executed']==False].sort_values(by=['TEvent','Fase']).iloc[0]['TEvent']
            # print("clock: " +str(self.clock))
            result = self.df_Events[(self.df_Events["executed"] == False) & (self.df_Events["TEvent"] == self.clock)]
        else:
            result=[]

        return str(self.clock), result
    
    def history(self):
        return self.df_Events