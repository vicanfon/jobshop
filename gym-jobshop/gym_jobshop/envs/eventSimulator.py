import pandas as pd
import numpy as np

# event structure: IdPedido TEvent Fase CodMaquina event executed

class eventSimulator():
    def __init__(self, Orders, Routes):
        # Initialize list of events
        Routes['n_pasos'] = Routes.groupby('CodPieza')['CodPieza'].transform('count')
        Routes['n_pasos_restantes'] = Routes['n_pasos']
        Routes['TTPreparacion'] = Routes.groupby('CodPieza')['TPreparacion'].transform('sum')
        Routes['TTUnitario'] = Routes.groupby('CodPieza')['TUnitario'].transform('sum')
        Orders = Orders.astype({'IdPedido': 'int64'})

        self.df_Events = Orders.join(Routes.set_index('CodPieza'), on='CodPieza')

        self.df_Events['indexEvent'] = self.df_Events['IdPedido'].astype(str) + "_" + self.df_Events['Fase'].astype(str)
        self.df_Events['executed'] = False
        self.df_Events = self.df_Events.set_index('indexEvent').rename(columns={'FechaPedido': 'TEvent'})
        self.df_Events['TEvent'] = pd.to_datetime(self.df_Events['TEvent'])
        self.df_Events['event'] = 1
        self.df_Events['TiempoProcesamiento'] = self.df_Events['TPreparacion'] + self.df_Events['TUnitario'] * \
                                        self.df_Events['Lote']
        self.df_Events['TiempoOcupacion'] = self.df_Events['TTPreparacion'] + self.df_Events['TTUnitario'] * \
                                    self.df_Events['Lote']
        self.df_Events['TiempoRestante'] = self.df_Events['TiempoOcupacion']
        self.df_Events['FechaCola'] = pd.NaT
        self.df_Events.TEvent[self.df_Events.Fase > 10] = pd.NaT

        self.clock = pd.NaT

    def createEvents(self, events, eventtype, clock):
        events['event'] = eventtype
        if eventtype == 2:
            events['executed'] = True
        else:
            events['executed'] = False
        events['TEvent'] = clock
        self.df_Events = self.df_Events.append(events)

    def processEvents(self, events):
        self.df_Events.loc[events.index, 'executed'] = True

    def addEvents(self, events):
        # add new events and mark some to executed
        # self.df_Events = self.df_Events.append(events[['IdPedido', 'TEvent', 'Fase', 'event','executed']])
        self.df_Events = self.df_Events.append(events)

    def nextEvents(self):
        # pick next events not processed
        eventsNonProcessed=self.df_Events[self.df_Events['executed'] == False].sort_values(by=['TEvent'])
        if len(eventsNonProcessed)>0:
            # take the time of the next event
            self.clock=eventsNonProcessed.iloc[0]['TEvent']
            # print("clock: " +str(self.clock))

        #return the next batch of events to process or an empty dataframe
        return self.clock, eventsNonProcessed[eventsNonProcessed["TEvent"] == self.clock]

    def updateEvent(self,index,attribute,value):    # this code solves the return of dataframes subsets by copy. If the caller has a reference to the dataframe dfc the value is change in the caller variable
        self.df_Events.loc[index,attribute]=value

    def history(self):
        return self.df_Events.copy(deep=True)