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
n_rules=15


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

    episode_over = False

    while not episode_over:
        # obs has the list of machines that are free to process new jobs
        selectedRules=[]
        for machine, row in obs.iterrows():
            # selectedRule = machinesNN[machine].selectJobNN(row)  # selected job is a rule here (1 of 16), not a specific one
            # selectedRules.append((machine,selectedRule))
            selectedRules.append((machine,10))
        obs, rewards, episode_over, info = env.step(selectedRules)
        # for machine, row in rewards.to_frame().iterrows():
        #   machinesNN[machine].setReward(rewards.loc[machine])

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
