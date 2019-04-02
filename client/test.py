import pandas as pd
import numpy as np
from random import randint

df_X= pd.DataFrame()
df_Y= pd.DataFrame(np.zeros((10000000, 1)), columns=['value'], dtype=int)
df_Z = pd.DataFrame(np.random.randn(100000, 2), columns=list('ab'))

def add_elements(n):
    global df_X

    for i in range(n):
        df_X = df_X.append([randint(0, 9)])

def mod_elements(n):
    for i in range(n):
        df_Y.value[i]=randint(0, 9)

def mod_elements2(n):  # superslow
    for i in range(n):
        df_Y.loc[i,'value']=randint(0, 9)

def read_data(n):
    for i in range(n):
        df_Y.loc[i,'value']

def read_data2(n):
    for i in range(n):
        df_Y.iloc[i,0]

def read_data3(n):    # superslow
    for i in range(n):
        df_Y[df_Y['value']==i]

def delete_data(n):    # superslow
    '''delete row using label'''
    for i in range(n):
        df_Y.drop([i])

def filter_data(n):   ## superslow
    '''delete row using label'''
    for i in range(n):
        df_Z.query('a > b')

def filter_data2(n):   # slow
    '''delete row using label'''
    for i in range(n):
        df_Z[df_Z.a > df_Z.b]  # same result as the previous expression

def filter_data3(n):
    '''delete row using label'''
    for i in range(n):
        df_Z[df_Z['a'] > df_Z['b']]  # same result as the previous expression
# add_elements(10000)
mod_elements(100)

# read_data(10000)
# read_data2(10000)
# read_data3(10000)
# delete_data(10000)  -- superslow
# filter_data(1000)
# filter_data2(10000)

# print(df_Y)
## parameters pass

def change_dataframe(df):  # dataframes are passed by reference
    df.value=1
    print(df)

class changeReturn():
    def __init__(self):
        self.dfc = pd.DataFrame(np.zeros((100, 1)), columns=['value'], dtype=int)

    def readDF(self):  # dataframes are passed by reference
        return self.dfc

    def filterDF(self):  # filters return a copy
        return self.dfc[self.dfc["value"] > 0]

    def filterDF2(self):  # filters return a copy
        # df.loc[selection criteria, columns I want] = value
        return self.dfc.loc[self.dfc.value > 0,'value']

    def filterDF3(self):  # filters return a copy
        # df.loc[selection criteria, columns I want] = value
        return self.dfc.iloc[(self.dfc.value > 0).values]

    def filterDF4(self):  # filters return ???
        # df.loc[selection criteria, columns I want] = value
        return self.dfc.at[self.dfc.value > 0]

    def filterDF5(self):  # filters return BY REFERENCE
        # df1.loc[lambda df: df.A > 0, :]
        return self.dfc.loc[lambda df:df.value > 0]

    def columnDF(self):  # returns a reference, not a copy
        return self.dfc["value"]

    def rowDF(self,i):  # returns a reference, not a copy
        return self.dfc.iloc[i]

    def rowDF2(self,i):  # returns a ???
        return self.dfc.loc[i]

    def writeDF(self,i,v):
        self.dfc.iloc[i,0]=v

    def writefilteredDF(self,v):    # this code solves the return of dataframes subsets by copy. If the caller has a reference to the dataframe dfc the value is change in the caller variable
        self.dfc.loc[self.dfc.value > 0,'value']=v


# change_dataframe(df_Y)
cReturn = changeReturn()
dfco= cReturn.readDF()
dfco.iloc[1,0]=9
dfco= cReturn.rowDF2(1)
dfco[0]=4
# cReturn.writefilteredDF(7)
def val_22():
    for i in range(10000):
        cReturn.filterDF5()

val_22()
dfco.iloc[:]=7

print(dfco)