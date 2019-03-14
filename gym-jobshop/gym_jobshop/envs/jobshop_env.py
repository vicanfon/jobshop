import  gym
from gym import spaces, Env
from gym import utils
from gym.utils import seeding
import pandas as pd
import numpy as np
from enum import Enum
from gym_jobshop.envs.eventSimulator import eventSimulator


class rules(Enum):
    FIFO = 0
    LIFO = 1
    SOT = 2
    LOT = 3
    SROT = 4
    LROT = 5
    LRO = 6
    MRO = 7
    DD = 8
    SS = 9
    DS = 10
    SSROT = 11
    DSROT = 12
    SSRO = 13
    DSRO = 14

class JobShopEnv(Env):
    metadata = {'render.modes': ['human']}

    def __init__(self):
        # self.viewer = None
        # coger una máquina y una de las 16 reglas de secuenciación
        self.action_space = spaces.Tuple((spaces.Discrete(5), spaces.Discrete(5))) # todo now five rules
        # state with lenght of the queue and  y del trabajo
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
        self.df_Orders = self.df_Orders.astype({'IdPedido': 'int64'})
        self._nMachines= len(self.df_Machines.index)
        self._nProducts= len(self.df_Products.index)

    def reset(self):
        # reset event simulator
        self.eventSimulator = eventSimulator(self.df_Orders, self.df_Routes)

        # Init the Global Queue and MachineProcessing
        self.MachineQueues = pd.DataFrame(columns={'IdPedido','Fase','CodMaquina','FechaPedido','FechaEntrega','FechaCola','TiempoProcesamiento','TiempoOcupacion','TiempoRestante','n_pasos','n_pasos_restantes'})
        self.MachineProcessing = pd.DataFrame(columns={'IdPedido'}, index=self.df_Machines.values[:,0], dtype=np.int8)
        self.MachineProcessing['IdPedido']=-1

        # Status of machine: queue_length and avg_waiting_time. Status of env: . As many rows as machines
        self.EnvState = pd.DataFrame(columns={'queue_length', 'avg_waiting_time'})

        return self.loop_of_events()

    def seed(self, seed=None):
        self.np_random, seed = gym.seeding.np_random(seed)
        return [seed]

    def __del__(self):
        pass

    def step(self, actions):
        # action is a tuple (machine, selectedRule, clock)
        for action in actions:
            job = self._selectJob(action[0],action[1])    # TODO: how to to this with a list
            self.MachineProcessing.loc[job['CodMaquina'],'IdPedido'] = job['IdPedido'].values
            # events, event, clock
            newEvents2 = self.eventSimulator.createEvents(job, 2, self.clock)
            self.eventSimulator.addEvents(newEvents2)
            self.eventSimulator.processEvents(newEvents2)
            # clock3 = (pd.to_datetime(action[2])+pd.to_timedelta(newEvents2.merge(job, left_on=['IdPedido'], right_on=['IdPedido'])['TiempoProcesamiento'],unit='m')).astype('datetime64[s]')
            clock3 = (pd.to_datetime(self.clock)+pd.to_timedelta(job['TiempoProcesamiento'],unit='m')).astype("datetime64[s]")
            # clock3 = clock3.round('s')
            self.eventSimulator.addEvents(self.eventSimulator.createEvents(job, 3, clock3))
            jobs1= job.copy(deep=True)
            jobs1['Fase'] += 10
            jobs1['n_pasos_restantes'] -= 1
            self.eventSimulator.addEvents(self.eventSimulator.createEvents(jobs1[jobs1['n_pasos_restantes']>=0], 1, clock3))
        # TODO: add history here to register what rule I have selected
        # reward = self._get_reward(self.clock)
        rewards = self._get_reward()
        obs = self.loop_of_events()
        episode_over = False if len(obs) > 0 else True
        # obs = obs if len(obs) > 0 else pd.DataFrame([(0.0, 0.0)], columns={'queue_length', 'avg_waiting_time'})

        return obs, rewards, episode_over, {}

    def _get_reward(self):
        """ Compute the av_waiting time """
        return - self.EnvState['avg_waiting_time'].copy(deep=True)

    def loop_of_events(self):
        self.clock, events = self.eventSimulator.nextEvents()

        while len(events) > 0:
            # event 1: load jobs that arrive at this time
            if len(events[events["event"] == 1]) > 0:
                self.assignJobs(events[events["event"] == 1], self.clock)

            # event 3: free the machine if a job just finished
            if len(events[events["event"] == 3]) > 0:
                self.freeMachine(events[events["event"] == 3], self.clock)  # free machine so that it can take more jobs

            obs = self.computeState(self.clock)
            if len(obs) > 0:
                return obs    # .copy(deep=True)
            else:
                self.clock, events = self.eventSimulator.nextEvents()
        return obs

    def assignJobs(self, events, clock):
        # Assign jobs to machine queue
        jobs = events.join(self.df_Orders.set_index('IdPedido'), on='IdPedido').merge(self.df_Routes, left_on=['CodPieza','Fase'], right_on=['CodPieza','Fase']).copy(deep=True)
        # jobs['TiempoProcesamiento'] = jobs['TPreparacion']+jobs['TUnitario']*jobs['Lote']
        jobs['FechaCola'] = pd.to_datetime(clock)
        jobs['TiempoProcesamiento'] = jobs['TPreparacion'] + jobs['TUnitario'] * \
                                                jobs['Lote']
        jobs['TiempoOcupacion'] = jobs['TTPreparacion'] + jobs['TTUnitario'] * \
                                                jobs['Lote']
        jobs['TiempoRestante'] = pd.to_datetime(jobs['TiempoOcupacion'])
        jobs['n_pasos_restantes'] = jobs['n_pasos']-(jobs['Fase'].astype('int')/10).astype('int')
        jobs = jobs[['IdPedido','Fase','CodMaquina','FechaPedido','FechaEntrega','FechaCola','TiempoProcesamiento','TiempoOcupacion','TiempoRestante','n_pasos','n_pasos_restantes']]
        # add new jobs
        self.MachineQueues = self.MachineQueues.append(jobs, ignore_index=True)

        self.eventSimulator.processEvents(events)

    def freeMachine(self, events, clock):
        # Assign Events to Machine queues
        self.eventSimulator.processEvents(events)  # update 3 to executed
        machines = events.join(self.df_Orders.set_index('IdPedido'), on='IdPedido').merge(self.df_Routes, left_on=['CodPieza','Fase'], right_on=['CodPieza','Fase'])['CodMaquina'].copy(deep=True)
        self.MachineProcessing.loc[machines,"IdPedido"] = -1


    def computeState(self, clock):
        # Compute state of the environment (aka statistics) after assigning work
        self.MachineQueues['TiempoEnCola'] = (pd.to_datetime(clock) - self.MachineQueues['FechaCola'])/ pd.Timedelta(hours=1)
        self.EnvState = self.MachineQueues.groupby(['CodMaquina']).agg({'Fase':'count','TiempoEnCola':'mean'}).rename(columns={'Fase': 'queue_length', 'TiempoEnCola': 'avg_waiting_time'}).copy(deep='True')

        # envstate: 'queue_length', 'avg_waiting_time',
        # computedResult: 'queue_length', 'avg_waiting_time',

        return self.EnvState.join(self.MachineProcessing).query("IdPedido == -1")[['avg_waiting_time','queue_length']].copy(deep=True) if len(self.EnvState.join(self.MachineProcessing))>0 else pd.DataFrame()


    def render(self, mode='human', close=False):
        """ Viewer only supports human mode currently. """
        pass

    def _selectJob(self, machine, idRule):
        queue = self.MachineQueues[self.MachineQueues['CodMaquina'] == machine]
        chosen = []
        # todo: compute dynamic attributes
        if idRule == rules.FIFO.value:
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

    def eventsHistory(self):
        return self.eventSimulator.history()