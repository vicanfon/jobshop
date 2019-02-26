import gym
import gym_jobshop
from gym_jobshop import *
import pandas as pd
import numpy as np
from machineNN import machineNN
# from eventSimulator import eventSimulator
import winsound
import matplotlib.pyplot as plt
from pathlib import Path



# create environment
env = gym.make('JobShop-v0')

# Read data and pass it to the environment
df_Machines = pd.read_csv("./data/Machines.csv", delimiter=',')
df_Orders = pd.read_csv("./data/Orders.csv", delimiter=',')
df_Products = pd.read_csv("./data/Products.csv", delimiter=',')
df_Routes = pd.read_csv("./data/Routes.csv", delimiter=',')


env.setEnv(df_Machines, df_Products, df_Routes, df_Orders)

n_obs=2
n_rules=5


# list of machineNN
machinesNN = {i[0]: machineNN(n_obs, n_rules) for i in df_Machines.values}

my_file = Path("modelM01.h5")
if my_file.is_file():
    for i in df_Machines['CodMaquina']:
        machinesNN[i].loadModel(i)

eSimulator = 0
reward_history = []

MAX_NUM_EPISODES = 20

for episode in range(MAX_NUM_EPISODES):
    print("episode: "+str(episode))
    # reset the environment and get the status of the environment
    obs = env.reset()
    
    # Get next tick and list of events on that tick
    clock, eventsList = env.nextEvents()
    
    while (len(eventsList)>0):
        eventsGroups=eventsList.groupby('CodMaquina')

        # for each machine with events process all its events
        for machine,jobs in eventsGroups:
            # event 1: load jobs that arrive at this time            
            if len(jobs[jobs["event"]==1])>0:
                env.assignJobs(machine, jobs[jobs["event"]==1], clock)
            
            # event 3: free the machine if a job just finished
            if len(jobs[jobs["event"]==3])>0:
                env.freeMachine(machine, clock) # free machine so that it can take more jobs
                # if there are jobs at the queue we can choose
                # job = jobs.loc[jobs["event"]==3,'IdPedido'].values[0]

            obs = env.computeState() # TODO: I am updating all states, how to optimize

            # if after assigning and liberating there are jobs and the machine is free select next job
            if obs.loc[machine, 'workingOn'] == -1 and obs.loc[machine, 'queue_length'] > 0:
                selectedRule = machinesNN[machine].selectJobNN(obs.loc[machine].drop(labels='workingOn'))   # selected job is a rule here (1 of 16), not a specific one
                nobs, reward, episode_over, info = env.step((machine, selectedRule, clock))       # pass action
                machinesNN[machine].trainNN(obs.loc[machine].drop(labels='workingOn'), nobs.loc[machine].drop(labels='workingOn'), reward)
                obs=nobs.copy(deep=True)
                # input event 2 for doc purposes
                # job = obs.loc[machine, 'workingOn']
                # eSimulator.addEvent(job, 2, clock, selectedRule)

        # next clock iteration
        clock, eventsList = env.nextEvents()
    time = eSimulator.history().iloc[-1].TEvent - eSimulator.history().iloc[0].TEvent
    totalReward=0
    for i in df_Machines['CodMaquina']:
        totalReward += machinesNN[i].getMachineTReward()
        machinesNN[i].resetMachineReward()

    print("time: "+str(time)+" - Reward: "+str(totalReward))
    reward_history.append(totalReward)
    


for i in df_Machines['CodMaquina']:
    machinesNN[i].saveModel(i)

plt.plot(reward_history)
plt.show()

eSimulator.history().to_excel("outputNN.xlsx")

print('finished')
winsound.PlaySound('SystemExclamation', winsound.SND_FILENAME)
