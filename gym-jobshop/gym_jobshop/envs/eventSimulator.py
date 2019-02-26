import pandas as pd
import numpy as np


class eventSimulator():
    def __init__(self, Orders, Routes):
        # Initialize list of events
        self.df_Events = Orders.join(Routes.set_index('CodPieza'), on='CodPieza').query('Fase == 10')[
            ['IdPedido', 'FechaPedido', 'Fase','CodMaquina']].copy(deep=True)
        self.df_Events['indexEvent'] = self.df_Events['IdPedido'].astype(str) + "_" + self.df_Events['Fase'].astype(str)
        self.df_Events['event'] = 1
        self.df_Events['executed'] = False
        self.df_Events = self.df_Events.set_index('indexEvent').rename(columns={'FechaPedido': 'TEvent'})
        self.df_Events.loc[:, 'TEvent'] = pd.to_datetime(self.df_Events.loc[:, 'TEvent'])

        self.clock = 0


    def addEvent(self, selectedJob, event, clock, selectedRule="None"):
        
        # add new events and mark some to executed
        if (event==2):
            newEvent=self.df_Events[self.df_Events['IdPedido']==selectedJob].sort_values(by=['TEvent', 'Fase']).query("executed==True").iloc[-1].copy(deep=True)
            newEvent['event']=2
            newEvent['selectedRule']=selectedRule
            newEvent['TEvent']=pd.to_datetime(clock)
            self.df_Events = self.df_Events.append(newEvent, ignore_index=True)
            newEvent['event']= 3
            newEvent['executed']=False
            # newEvent['selectedRule']="None"
            newEvent['TEvent']= pd.to_datetime(newEvent['TEvent']) + pd.DateOffset(minutes=newEvent['TPreparacion'] + newEvent['Lote'] * newEvent['TUnitario'])
            newEvent['TEvent'] = newEvent['TEvent'].round('s')
            self.df_Events = self.df_Events.append(newEvent, ignore_index=True).sort_values(by=['TEvent', 'Fase'])
        elif (event==3):
            self.df_Events.loc[(self.df_Events['IdPedido']==selectedJob) & (self.df_Events['event']==3) & (self.df_Events['executed']==False),'executed']=True
            newEvent = self.df_Events.loc[(self.df_Events['IdPedido']==selectedJob) & (self.df_Events['event']==3)].sort_values(by=['TEvent', 'Fase']).iloc[-1]
            # add here next event (phase)
            newFase = newEvent['Fase'] + 10
            nextEvent=self.df_Orders[self.df_Orders['IdPedido']==selectedJob].join(self.df_Routes.set_index('CodPieza'), on='CodPieza').query('Fase == '+ str(newFase))
            if len(nextEvent)>0:
                nextEvent['event']=1
                nextEvent['executed']=False
                nextEvent['selectedRule']="None"
                nextEvent=nextEvent.rename(columns = {'FechaPedido':'TEvent'})
                nextEvent['TEvent']= newEvent['TEvent']

                self.df_Events = self.df_Events.append(nextEvent, ignore_index=True).sort_values(by=['TEvent', 'Fase']).reset_index(drop=True)
                # print("added pedido:" + str(nextEvent['IdPedido'].values[0]) + " fase : " + str(nextEvent['Fase'].values[0]))
    
    def markExecuted(self, events):
        self.df_Events.loc[events.index, 'executed'] = True
        
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