import numpy as np
from keras.models import Sequential
from keras.layers import Dense, InputLayer


class machineNN():
    def __init__(self, n_inputs, n_outputs):
        # create the keras model
        self.n_inputs = n_inputs
        self.n_outputs = n_outputs
        self.model = Sequential()
        self.model.add(InputLayer(batch_input_shape=(1, n_inputs)))
        self.model.add(Dense(10, activation='sigmoid'))
        self.model.add(Dense(n_outputs, activation='linear'))
        self.model.compile(loss='mse', optimizer='adam', metrics=['mae'])
        self.y = 0.95
        self.eps = 0.2
        self.decay_factor = 0.999
        self.reward = 0

    def selectJob(self, machineStatus):
        return 2
    
    def adjustEps(self):
        self.eps *= self.decay_factor
    
    def selectJobNN(self, machinesStatus):
        if np.random.random() < self.eps:
            a = np.random.randint(1, self.n_outputs-1)
        else:
            a = np.argmax(self.model.predict(machinesStatus.values.reshape(1,self.n_inputs)))
        return a
    
    def trainNN(self, machinesStatus, new_machinesStatus, r):
        a = np.argmax(self.model.predict(machinesStatus.values.reshape(1,self.n_inputs)))
        target = r + self.y * np.max(self.model.predict(new_machinesStatus.values.reshape(1,self.n_inputs)))
        target_vec = self.model.predict(machinesStatus.values.reshape(1,self.n_inputs))[0]
        target_vec[a] = target
        self.model.fit(machinesStatus.values.reshape(1,self.n_inputs), target_vec.reshape(-1, self.n_outputs), epochs=1, verbose=0)
        self.reward += r
    
    def getMachineTReward(self):
        return self.reward
    
    def resetMachineReward(self):
        self.reward = 0

    def getStats(self):
        # compute stats on time, occupation, etc
        pass
    
    def saveModel(self, machine):
        self.model.save_weights("model"+machine+".h5")
        print("Saved model to disk")

    def loadModel(self, machine):
        self.model.load_weights("model"+machine+".h5")
        print("Loaded model from disk")

