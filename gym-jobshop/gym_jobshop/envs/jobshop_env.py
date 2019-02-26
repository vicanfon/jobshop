import os, subprocess, time, signal
# import gym
import  gym
from gym import spaces, Env
from gym import utils
from gym.utils import seeding
import pandas as pd
import numpy as np
import logging
logger = logging.getLogger(__name__)

from gym_jobshop.envs.jobshopUtils import Machine
from gym_jobshop.envs.eventSimulator import eventSimulator

class JobShopEnv(Env):
    metadata = {'render.modes': ['human']}

    def __init__(self):
        # self.viewer = None
        # coger una máquina y una de las 16 reglas de secuenciación
        self.action_space = spaces.Tuple((spaces.Discrete(5), spaces.Discrete(5))) # todo now five rules
        # state with lenght of the queue and  y del trabajo
        # self.observation_space = spaces.Box(0,np.inf, shape=(1,0), dtype = np.int16), "av_waiting_time": spaces.Box(0,np.inf, shape=(1,0), dtype = np.int16)})
        self.observation_space = spaces.Box(0,np.inf, shape=(10,3), dtype = np.int16) # 'queue_length','avg_waiting_time', 'workingOn'
        # self.seed()


    def setEnv(self, machines, products, routes, orders):
        """
        Receives four pandas dataframes with data on machines, products, orders and routes
        """
        self.df_Machines = machines
        self.df_Products = products
        self.df_Routes = routes
        self.df_Routes['n_pasos'] = self.df_Routes.groupby('CodPieza')['CodPieza'].transform('count')
        self.df_Routes['TTPreparacion'] = self.df_Routes.groupby('CodPieza')['TPreparacion'].transform('sum')
        self.df_Routes['TTUnitario'] = self.df_Routes.groupby('CodPieza')['TUnitario'].transform('sum')
        self.df_Orders = orders
        self._nMachines= len(self.df_Machines.index)
        self._nProducts= len(self.df_Products.index)

    def seed(self, seed=None):
        self.np_random, seed = gym.seeding.np_random(seed)
        return [seed]

    def __del__(self):
        pass

    def step(self, action):
        # action is a tuple (machine, rule)

        job = self.Buffer[action[0]].selectJob(action[1])
        
        self._computeState()
        obs = self.EnvState.copy(deep=True)
        reward = self._get_reward()
        episode_over = False
        # update the event 2
        self.eventSimulator.addEvent(job, 2, action[2], action[1])

        return obs, reward, episode_over, {}

    def _get_reward(self):
        """ Compute the av_waiting time """
        # print (self.EnvState['avg_waiting_time'].mean())
        return - self.EnvState['avg_waiting_time'].mean()

    def reset(self):
        # reset event simulator
        self.eventSimulator = eventSimulator(self.df_Orders, self.df_Routes)

        # Initialize status of the environment. Status of machine: queue_length and avg_waiting_time. Status of env: todo
        self.EnvState = pd.DataFrame(columns={'queue_length','avg_waiting_time', 'workingOn'},index=self.df_Machines.values[:,0])
        self.EnvState['queue_length']=0
        self.EnvState['avg_waiting_time']=0
        self.EnvState['workingOn']=0
        # Initialize machine parameters
        self.Buffer = {i[0]: Machine(i[0]) for i in self.df_Machines.values}

        return self.EnvState

    def getEvents(self):
        # Get next block of events
        self.clock=self.df_Events[self.df_Events['executed']==False].sort_values(by=['TEvent','Fase']).iloc[0]['TEvent']

        return self.df_Events.query("executed == False & TEvent == '" + self.clock +"'")[['TEvent','event','IdPedido','CodPieza','CodMaquina']]
    
    def assignJobs(self, machine, events, clock):
        # Assign jobs to machine queue
        jobs = events.join(self.df_Orders.set_index('IdPedido'), on='IdPedido').merge(self.df_Routes, left_on=['CodPieza','Fase','CodMaquina'], right_on=['CodPieza','Fase','CodMaquina']).copy(deep=True)
        # jobs['TiempoProcesamiento'] = jobs['TPreparacion']+jobs['TUnitario']*jobs['Lote']
        jobs['FechaCola'] = clock
        jobs['TiempoProcesamiento'] = jobs['TPreparacion'] + jobs['TUnitario'] * \
                                                jobs['Lote']
        jobs['TiempoOcupacion'] = jobs['TTPreparacion'] + jobs['TTUnitario'] * \
                                                jobs['Lote']
        jobs['TiempoRestante'] = jobs['TiempoOcupacion']
        jobs['n_pasos_restantes'] = jobs['n_pasos']
        jobs = jobs[['IdPedido','FechaPedido','FechaEntrega','FechaCola','TiempoProcesamiento','TiempoOcupacion','TiempoRestante','n_pasos','n_pasos_restantes']]
        # add new jobs
        self.Buffer[machine].queue= self.Buffer[machine].queue.append(jobs, ignore_index=True)

        # self.Buffer[i[6]].queue=self.Buffer[i[6]].queue.append({'id':i[0],'phase':i[7],'lote':i[2],'tp': i[8],'tu': i[9],'queueDate':clock,'arrivalDate':i[3],'idOrder':i[0],'operationTime': i[8]+i[2]*i[9],'deliverDate':i[4], 'remainingSteps': i[10]-int(i[7]/10)}, ignore_index=True) # assign piece to queue

        self.eventSimulator.markExecuted(events)


    def freeMachine(self, machine, clock):
        # Assign Events to Machine queues
        self.eventSimulator.addEvent(self.Buffer[machine].processingJob, 3, clock)  # update 3 to executed
        self.Buffer[machine].processingJob = -1


    def computeState(self):
        # Compute state of the environment (aka statistics) after assigning work
        for i in self.Buffer:
            length = len(self.Buffer[i].queue)
            self.EnvState.loc[i,'queue_length']= length
            if length > 0:
                clock2=pd.to_datetime(self.Buffer[i].queue['FechaCola']).sort_values(ascending = False).iloc[0]
                difference=clock2-pd.to_datetime(self.Buffer[i].queue['FechaCola'])
                self.EnvState.loc[i,'avg_waiting_time']=difference.mean().seconds
            else:
                self.EnvState.loc[i,'avg_waiting_time']=0
            self.EnvState.loc[i,'workingOn']=self.Buffer[i].processingJob

        return self.EnvState.copy(deep=True)


    def render(self, mode='human', close=False):
        """ Viewer only supports human mode currently. """
        pass

    def nextEvents(self):
        return self.eventSimulator.nextEvents()