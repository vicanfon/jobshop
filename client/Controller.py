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
n_rules=4


# machineNN
machinesNN = {i[0]: machineNN(n_obs, n_rules) for i in df_Machines.values}

my_file = Path("modelM01.h5")
if my_file.is_file():
    for i in df_Machines['CodMaquina']:
        machinesNN[i].loadModel(i)

reward_history = []

MAX_NUM_EPISODES = 1

for episode in range(MAX_NUM_EPISODES):
    print("episode: "+str(episode))
    # reset the environment and get the status of the environment
    obs = env.reset()
    
    # Get next tick and list of events on that tick
    clock, events = env.nextEvents()

    while (len(events) > 0):
        # TODO: could I move this code to env.reset before returning the obs and let only the lines after obs
        # event 1: load jobs that arrive at this time
        if len(events[events["event"] == 1]) > 0:
            env.assignJobs(events[events["event"] == 1], clock)

        # event 3: free the machine if a job just finished
        if len(events[events["event"] == 3]) > 0:
            env.freeMachine(events[events["event"] == 3], clock)  # free machine so that it can take more jobs

        obs = env.computeState(clock)

        selectedRules=[]
        # obs has the list of machines that are free to process new jobs
        for machine, row in obs.iterrows():
            selectedRule = machinesNN[machine].selectJobNN(row)  # selected job is a rule here (1 of 16), not a specific one
            selectedRules.append((machine,selectedRule,clock))
        nobs, reward, episode_over, info = env.step(selectedRules)  # TODO: pass array of selectedRules with all the machines at the same time
            # machinesNN[machine].trainNN(row, nobs, reward)   # TODO: aqui devuelvo 0, nada, que hago?

        # next clock iteration
        clock, events = env.nextEvents()
    eventsHistory = env.eventsHistory()
    time = eventsHistory.iloc[-1].TEvent - eventsHistory.iloc[0].TEvent
    env.eventsHistory().to_excel("outputNN.xlsx")
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

env.eventsHistory().to_excel("outputNN.xlsx")

print('finished')
winsound.PlaySound('SystemExclamation', winsound.SND_FILENAME)
