import gym
import gym_jobshop
from gym_jobshop import *
import pandas as pd
import numpy as np
from machineNN import machineNN
from eventSimulator import eventSimulator
import winsound
import matplotlib.pyplot as plt


# create environment
env = gym.make('JobShop-v0')

# Read data and pass it to the environment
df_Machines = pd.read_csv("./data/Machines.csv", delimiter=',')
df_Orders = pd.read_csv("./data/Orders.csv", delimiter=',')
df_Products = pd.read_csv("./data/Products.csv", delimiter=',')
df_Routes = pd.read_csv("./data/Routes.csv", delimiter=',')


env.setEnv(df_Machines, df_Products, df_Routes)

n_obs=2
n_rules=3


# list of machineNN
machinesNN = {i[0]: machineNN(n_obs, n_rules) for i in df_Machines.values}
for i in df_Machines['CodMaquina']:
    machinesNN[i].loadModel(i)

eSimulator = 0
reward_history = []

MAX_NUM_EPISODES = 6

for episode in range(MAX_NUM_EPISODES):
    print("episode: "+str(episode))
    eSimulator= eventSimulator()
    # reset the environment and get the status of the environment
    obs = env.reset()
    # update eps on the Q-learning
    
    # Compute line of events
    eSimulator.initializeEvents(df_Orders, df_Routes)  # panda df with events. Structure: TEvent, event, IdPedido, CodPieza, CodMaquina . Events are not executed
    clock, eventsList = eSimulator.nextEvents()
    
    while (len(eventsList)>0):
        eventsGroups=eventsList.groupby('CodMaquina')

        # process each machine
        for machine,jobs in eventsGroups:
            # print("Processing "+ machine)
            # event 1: load jobs that arrive at this time            
            if len(jobs[jobs["event"]==1])>0:  #if this machine has a three event in this tick
                obs = env.assignJobs(machine, jobs[jobs["event"]==1], clock)
                eSimulator.addEvents(jobs[jobs["event"]==1])    # update 3 to executed
            
            # event 3: free the machine if a job just finished
            if len(jobs[jobs["event"]==3])>0:  #if this machine has a three event in this tick
                obs = env.freeMachine(machine) # free machine so that it can take more jobs
                # if there are jobs at the queue we can choose
                job = jobs.loc[jobs["event"]==3,'IdPedido'].values[0]
                eSimulator.addEvent(job, 3, clock)    # update 3 to executed
            
            # if after assigning and liberating there are jobs and the machine is free select next job
            if obs.loc[machine, 'workingOn'] == -1 and obs.loc[machine, 'queue_length'] > 0:
                selectedRule = machinesNN[machine].selectJobNN(obs.loc[machine].drop(labels='workingOn'))   # selected job is a rule here (1 of 16), not a specific one
                nobs, reward, episode_over, info = env.step((machine, selectedRule))       # pass action
                machinesNN[machine].trainNN(obs.loc[machine].drop(labels='workingOn'), nobs.loc[machine].drop(labels='workingOn'), reward)
                obs=nobs.copy(deep=True)
                # input event 2 for doc purposes
                job = obs.loc[machine, 'workingOn']
                eSimulator.addEvent(job, 2, clock, selectedRule)

        # next clock iteration
        clock, eventsList = eSimulator.nextEvents()
    time = eSimulator.history().iloc[-1].TEvent - eSimulator.history().iloc[0].TEvent
    totalReward=0
    for i in df_Machines['CodMaquina']:
        totalReward += machinesNN[i].getMachineTReward()
    print("time: "+str(time)+" - Reward: "+str(totalReward))
    reward_history.append(totalReward)
    

    
    # all the events are processed
    # env.showStats()
    #display(eSimulator.history())
eSimulator.history().to_excel("outputNN.xlsx")
for i in df_Machines['CodMaquina']:
    machinesNN[i].saveModel(i)

plt.plot(reward_history)
plt.show()

print('finished')
winsound.PlaySound('SystemExclamation', winsound.SND_FILENAME)
