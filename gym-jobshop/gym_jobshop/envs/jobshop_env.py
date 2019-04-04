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
        # self.df_Products = products
        self.df_Routes = routes
        self.df_Orders = orders
        # self._nMachines= len(self.df_Machines.index)
        # self._nProducts= len(self.df_Products.index)

    def reset(self):
        # reset event simulator
        self.eventSimulator = eventSimulator(self.df_Orders, self.df_Routes)

        # Init the Global Queue and MachineProcessing
        self.History = pd.DataFrame()
        self.MachineQueues = pd.DataFrame()
        self.MachineProcessing = pd.DataFrame(columns={'IdPedido'}, index=self.df_Machines.values[:,0], dtype=np.int8)
        self.MachineProcessing['IdPedido']=-1

        # Status of machine: queue_length and avg_waiting_time. Status of env: . As many rows as machines
        self.EnvState = pd.DataFrame(columns={'queue_length', 'avg_waiting_time'})

        return self.loop_of_events()

    def _seed(self, seed=None):
        self.np_random, seed = gym.seeding.np_random(seed)
        return [seed]

    def __del__(self):
        pass

    def step(self, actions):
        # action is a tuple (machine, selectedRule, clock)
        for action in actions:
            pickedJob = self._selectJob(action[0],action[1],self.clock)    # TODO: how to to this with a list
            self.MachineProcessing.loc[pickedJob['CodMaquina'],'IdPedido'] = pickedJob['IdPedido'].values
            # events, event, clock
            self.eventSimulator.createEvents(pickedJob, 2, self.clock)

            clock3 = (pd.to_datetime(self.clock)+pd.to_timedelta(pickedJob['TiempoProcesamiento'],unit='m')).astype("datetime64[s]")
            # clock3 = clock3.round('s')
            self.eventSimulator.createEvents(pickedJob, 3, clock3)
            jobs1= pickedJob    #.copy(deep=True)
            jobs1['Fase'] += 10
            jobs1['TiempoRestante'] -= jobs1['TiempoProcesamiento']
            jobs1['n_pasos_restantes'] -= 1
            # jobs1['TEvent'] = clock3
            jobs1['indexEvent'] = jobs1['IdPedido'].astype(str) + "_" + jobs1['Fase'].astype(str)
            jobs1 = jobs1.set_index('indexEvent')
            if len(jobs1[jobs1['n_pasos_restantes']>0])>0:
                self.eventSimulator.updateEvent(jobs1[jobs1['n_pasos_restantes']>=0].index,['TiempoRestante','n_pasos_restantes','TEvent'], jobs1[['TiempoRestante','n_pasos_restantes','TEvent']])

        rewards = self._get_rewards()
        obs = self.loop_of_events()
        episode_over = False if len(obs) > 0 else True
        # obs = obs if len(obs) > 0 else pd.DataFrame([(0.0, 0.0)], columns={'queue_length', 'avg_waiting_time'})

        return obs, rewards, episode_over, {}

    def _get_rewards(self):
        """ Compute the av_waiting time """
        return - self.EnvState['avg_waiting_time'].copy(deep=True)

    def loop_of_events(self):
        self.clock, events = self.eventSimulator.nextEvents()

        while len(events) > 0:
            # event 3: free the machine if a job just finished
            if len(events[events["event"] == 3]) > 0:
                # the event says that machines should be free for processing new jobs
                # update event simulator
                self.eventSimulator.updateEvent(events[events.event == 3].index, 'executed', True)
                self.MachineProcessing.loc[events[events["event"] == 3].CodMaquina,"IdPedido"] = -1
                # update local
                # events.executed[events.event == 3] = True
                # self.MachineQueues=self.MachineQueues.append(events[events["event"] == 3])

            # event 1: load jobs that arrive at this time
            if len(events[events["event"] == 1]) > 0:
                # update event
                self.eventSimulator.updateEvent(events[events.event == 1].index, 'executed', True)
                self.eventSimulator.updateEvent(events[events.event == 1].index, 'FechaCola', pd.to_datetime(self.clock))  # informative not for decision making
                # update in local so we can make decisions
                events.FechaCola[events.event == 1] = pd.to_datetime(self.clock) # for decision making
                events.executed[events.event == 1] = True
                self.MachineQueues=self.MachineQueues.append(events[events.event == 1])

            # Compute list of machines queues with a queue with a job of the environment (aka statistics) after assigning work
            self.MachineQueues['TiempoEnCola'] = (pd.to_datetime(self.clock) - self.MachineQueues[
                    'FechaCola']) / pd.Timedelta(hours=1)
            # get machines free so an action can be picked
            self.EnvState = self.MachineQueues.groupby(['CodMaquina']).agg(
                    {'Fase': 'count', 'TiempoEnCola': 'mean'}).rename(
                    columns={'Fase': 'queue_length', 'TiempoEnCola': 'avg_waiting_time'}) #.copy(deep='True')
            obs = self.EnvState.join(self.MachineProcessing).query("IdPedido == -1")[
                    ['avg_waiting_time', 'queue_length']] if len(
                    self.EnvState.join(self.MachineProcessing)) > 0 else pd.DataFrame()
            # if there are free I return, if not I process more events in the meantime
            if len(obs) > 0:
                return obs    # .copy(deep=True)
            else:
                self.clock, events = self.eventSimulator.nextEvents()
        return obs

    def _render(self, mode='human', close=False):
        """ Viewer only supports human mode currently. """
        pass

    def _selectJob(self, machine, idRule, now):
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
            chosen = pd.DataFrame([queue.sort_values(by=['TiempoRestante']).iloc[0]])
        elif idRule == rules.LROT.value:
            chosen = pd.DataFrame([queue.sort_values(by=['TiempoRestante']).iloc[-1]])
        elif idRule == rules.LRO.value:
            chosen = pd.DataFrame([queue.sort_values(by=['n_pasos_restantes']).iloc[0]])
        elif idRule == rules.MRO.value:
            chosen = pd.DataFrame([queue.sort_values(by=['n_pasos_restantes']).iloc[-1]])
        elif idRule == rules.DD.value:
            chosen = pd.DataFrame([queue.sort_values(by=['FechaEntrega']).iloc[0]])
        elif idRule == rules.SS.value:
            queue['ss']=pd.to_datetime(queue['FechaEntrega'])-now
            chosen = pd.DataFrame([queue.sort_values(by=['ss']).iloc[0]])
        elif idRule == rules.DS.value:
            queue['ds'] = pd.to_datetime(queue['FechaEntrega']) - now -  pd.to_timedelta(queue['TiempoRestante'],unit='m') # pd.Timedelta(queue['TiempoRestante'].astype(str).values[0]+" min")
            chosen = pd.DataFrame([queue.sort_values(by=['ds']).iloc[0]])
        elif idRule == rules.SSROT.value:
            queue['ssrot'] = (pd.to_datetime(queue['FechaEntrega']) - now)/pd.to_timedelta(queue['TiempoRestante'],unit='m')
            chosen = pd.DataFrame([queue.sort_values(by=['ssrot']).iloc[0]])
        elif idRule == rules.DSROT.value:
            queue['dsrot'] = (pd.to_datetime(queue['FechaEntrega']) - now - pd.to_timedelta(queue['TiempoRestante'],unit='m'))/pd.to_timedelta(queue['TiempoRestante'],unit='m')
            chosen = pd.DataFrame([queue.sort_values(by=['dsrot']).iloc[0]])
        elif idRule == rules.SSRO.value:
            queue['ssro'] = (pd.to_datetime(queue['FechaEntrega']) - now) / queue['n_pasos_restantes']
            chosen = pd.DataFrame([queue.sort_values(by=['ssro']).iloc[0]])
        elif idRule == rules.DSRO.value:
            queue['dsro'] = (pd.to_datetime(queue['FechaEntrega']) - now - pd.to_timedelta(queue['TiempoRestante'],unit='m')) / \
                            (queue['n_pasos_restantes']-1)
            chosen = pd.DataFrame([queue.sort_values(by=['dsro']).iloc[0]])
        # self.processingJob = chosen.copy(deep=True)
        self.MachineQueues.drop(chosen.index, inplace=True)
        # processingJob = chosen['IdPedido'].values[0]

        return chosen

    def eventsHistory(self):
        return self.eventSimulator.history()