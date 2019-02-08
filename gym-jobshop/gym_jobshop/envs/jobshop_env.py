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

class JobShopEnv(Env):
    metadata = {'render.modes': ['human']}

    def __init__(self):
        # self.viewer = None
        # coger una máquina y una de las 16 reglas de secuenciación
        self.action_space = spaces.Tuple((spaces.Discrete(5), spaces.Discrete(3))) # todo now three rules
        # state with lenght of the queue and  y del trabajo
        # self.observation_space = spaces.Box(0,np.inf, shape=(1,0), dtype = np.int16), "av_waiting_time": spaces.Box(0,np.inf, shape=(1,0), dtype = np.int16)})
        self.observation_space = spaces.Box(0,np.inf, shape=(10,3), dtype = np.int16) # 'queue_length','avg_waiting_time', 'workingOn'
        # self.seed()
    
    def setEnv(self, machines, products, routes):
        """
        Receives four pandas dataframes with data on machines, products, orders and routes
        """
        self.df_Machines = machines
        self.df_Products = products
        self.df_Routes = routes
        self._nMachines= len(self.df_Machines.index)
        self._nProducts= len(self.df_Products.index)

    def seed(self, seed=None):
        self.np_random, seed = gym.seeding.np_random(seed)
        return [seed]

    def __del__(self):
        pass

    def step(self, action):
        # action is a tuple (machine, rule)

        self.Buffer[action[0]].selectJob(action[1])
        
        self._computeState()
        obs = self.EnvState.copy(deep=True)
        reward = self._get_reward()
        episode_over = False

        return obs, reward, episode_over, {}

    def _get_reward(self):
        """ Compute the av_waiting time """
        return - self.EnvState['avg_waiting_time'].mean()

    def reset(self):
        # Initialize status of the environment. Status of machine: queue_length and avg_waiting_time. Status of env: todo
        self.EnvState = pd.DataFrame(columns={'queue_length','avg_waiting_time', 'workingOn'},index=self.df_Machines.values[:,0])
        self.EnvState['queue_length']=0
        self.EnvState['avg_waiting_time']=0
        self.EnvState['workingOn']=0
        # Initialize machine parameters
        self.Buffer = {i[0]: Machine(i[0]) for i in self.df_Machines.values}

        for i in self.Buffer:
            self.EnvState.loc[i,'queue_length']=len(self.Buffer[i].queue)

        return self.EnvState

    def getEvents(self):
        # Get next block of events
        self.clock=self.df_Events[self.df_Events['executed']==False].sort_values(by=['TEvent','Fase']).iloc[0]['TEvent']

        return self.df_Events.query("executed == False & TEvent == '" + self.clock +"'")[['TEvent','event','IdPedido','CodPieza','CodMaquina']]
    
    def assignJobs(self, machine, jobs, clock):
        # Assign Events to Machine queues
        # jobsExtended = self.df_Orders[self.df_Orders['IdPedido'].isin(jobs['IdPedido'])].join(self.df_Routes.set_index('CodPieza'), on='CodPieza')  # reset_index().
        for i in jobs.values:
            self.Buffer[i[6]].queue=self.Buffer[i[6]].queue.append({'id':i[0],'phase':i[7],'lote':i[2],'tp': i[8],'tu': i[9],'queueDate':clock,'arrivalDate':i[3],'idOrder':i[0],'deliverDate':i[4]}, ignore_index=True) # assign piece to queue
        
        self._computeState()

        return self.EnvState.copy(deep=True)
    
    def freeMachine(self, machine):
        # Assign Events to Machine queues
        self.Buffer[machine].processingJob = -1
        self._computeState()

        return self.EnvState.copy(deep=True)

    def _computeState(self):
        # Compute state of the environment (aka statistics) after assigning work
        for i in self.Buffer:
            length = len(self.Buffer[i].queue)
            self.EnvState.loc[i,'queue_length']= length
            if length > 0:
                clock2=pd.to_datetime(self.Buffer[i].queue['arrivalDate']).iloc[0]
                difference=pd.to_datetime(self.Buffer[i].queue['arrivalDate'])-clock2
                self.EnvState.loc[i,'avg_waiting_time']=difference.mean().seconds
            else:
                self.EnvState.loc[i,'avg_waiting_time']=0
            self.EnvState.loc[i,'workingOn']=self.Buffer[i].processingJob

    def render(self, mode='human', close=False):
        """ Viewer only supports human mode currently. """
        pass