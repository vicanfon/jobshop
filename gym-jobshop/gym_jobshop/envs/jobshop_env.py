import  gym
from gym import spaces, Env
from gym import utils
from gym.utils import seeding
import pandas as pd
import numpy as np
from enum import Enum
from gym_jobshop.envs.eventSimulator import eventSimulator


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
        # action is a tuple (machine, selectedRule, clock)

        job = self._selectJob(action[0],action[1])
        obs = self.computeState(action[2])
        reward = self._get_reward(action[0])
        episode_over = False
        # update the event 2
        # events, event, clock
        self.eventSimulator.addEvent(job, 2, action[2])
        # TODO: add history here to register what rule I have selected

        return obs, reward, episode_over, {}

    def _get_reward(self, machine):
        """ Compute the av_waiting time """
        # print (self.EnvState['avg_waiting_time'].mean())
        if len(self.EnvState)>0 and len(self.EnvState.loc[machine])>0:
            return - self.EnvState.loc[machine,'avg_waiting_time']
        else:
            return 0

    def reset(self):
        # reset event simulator
        self.eventSimulator = eventSimulator(self.df_Orders, self.df_Routes)

        # Init the Global Queue and MachineProcessing
        self.MachineQueues = pd.DataFrame(columns={'IdPedido','Fase','CodMaquina','FechaPedido','FechaEntrega','FechaCola','TiempoProcesamiento','TiempoOcupacion','TiempoRestante','n_pasos','n_pasos_restantes'})
        self.MachineProcessing = pd.DataFrame(columns={'IdPedido'}, index=self.df_Machines.values[:,0], dtype=np.int8)
        self.MachineProcessing['IdPedido']=-1

        # Initialize status of the environment. Status of machine: queue_length and avg_waiting_time. Status of env: todo
        # self.EnvState = pd.DataFrame(columns={'queue_length','avg_waiting_time'},index=self.df_Machines.values[:,0])
        # self.EnvState['queue_length']=0
        # self.EnvState['avg_waiting_time']=0
        self.EnvState = pd.DataFrame(columns={'queue_length', 'avg_waiting_time'})

        return self.EnvState.copy(deep=True)

    def getEvents(self):
        # Get next block of events
        self.clock=self.df_Events[self.df_Events['executed']==False].sort_values(by=['TEvent','Fase']).iloc[0]['TEvent']

        return self.df_Events.query("executed == False & TEvent == '" + self.clock +"'")[['TEvent','event','IdPedido','CodPieza','CodMaquina']]
    
    def assignJobs(self, events, clock):
        # Assign jobs to machine queue
        jobs = events.join(self.df_Orders.set_index('IdPedido'), on='IdPedido').merge(self.df_Routes, left_on=['CodPieza','Fase','CodMaquina'], right_on=['CodPieza','Fase','CodMaquina']).copy(deep=True)
        # jobs['TiempoProcesamiento'] = jobs['TPreparacion']+jobs['TUnitario']*jobs['Lote']
        jobs['FechaCola'] = pd.to_datetime(clock)
        jobs['TiempoProcesamiento'] = jobs['TPreparacion'] + jobs['TUnitario'] * \
                                                jobs['Lote']
        jobs['TiempoOcupacion'] = jobs['TTPreparacion'] + jobs['TTUnitario'] * \
                                                jobs['Lote']
        jobs['TiempoRestante'] = pd.to_datetime(jobs['TiempoOcupacion'])
        jobs['n_pasos_restantes'] = jobs['n_pasos']
        jobs = jobs[['IdPedido','Fase','CodMaquina','FechaPedido','FechaEntrega','FechaCola','TiempoProcesamiento','TiempoOcupacion','TiempoRestante','n_pasos','n_pasos_restantes']]
        # add new jobs
        self.MachineQueues = self.MachineQueues.append(jobs, ignore_index=True)

        self.eventSimulator.addEvent(events, 1, clock)


    def freeMachine(self, events, clock):
        # Assign Events to Machine queues
        self.eventSimulator.addEvent(events, 3, clock)  # update 3 to executed
        self.MachineProcessing.loc[events.index,"IdPedido"] = -1

    def computeState(self, clock):
        # Compute state of the environment (aka statistics) after assigning work
        self.MachineQueues['TiempoEnCola'] = (pd.to_datetime(clock) - self.MachineQueues['FechaCola'])/ pd.Timedelta(hours=1)
        self.EnvState = self.MachineQueues.groupby(['CodMaquina']).agg({'Fase':'count','TiempoEnCola':'mean'}).rename(columns={'Fase': 'queue_length', 'TiempoEnCola': 'avg_waiting_time'}).copy(deep='True')

        # envstate: 'queue_length', 'avg_waiting_time',
        # computedResult: 'queue_length', 'avg_waiting_time',

        return self.EnvState.join(self.MachineProcessing).query("IdPedido == -1")[['avg_waiting_time','queue_length']].copy(deep=True)


    def render(self, mode='human', close=False):
        """ Viewer only supports human mode currently. """
        pass

    def nextEvents(self):
        return self.eventSimulator.nextEvents()

    def _selectJob(self, machine, idRule):
        queue = self.MachineQueues[self.MachineQueues['CodMaquina'] == machine]
        chosen = []
        # todo: compute dynamic attributes
        if idRule == rules.RANDOM.value:
            chosen = queue.sample(n=1)
        elif idRule == rules.FIFO.value:
            chosen = pd.DataFrame([queue.sort_values(by=['FechaCola']).iloc[0]])
        elif idRule == rules.LIFO.value:
            chosen = pd.DataFrame([queue.sort_values(by=['FechaCola']).iloc[-1]])
        elif idRule == rules.SOT.value:
            chosen = pd.DataFrame([queue.sort_values(by=['TiempoProcesamiento']).iloc[0]])
        elif idRule == rules.LOT.value:
            chosen = pd.DataFrame([queue.sort_values(by=['TiempoProcesamiento']).iloc[-1]])
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
        self.MachineQueues.drop(chosen.index, inplace=True)
        # processingJob = chosen['IdPedido'].values[0]

        return chosen