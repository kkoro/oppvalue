# -*- coding: utf-8 -*-
"""
Clean ccma (CRSP-Compustat Merged) dataset from WRDS query and add to sqlite
database

Created on Sun Aug 21 20:29:43 2016

@author: ltakeuchi
"""

import pandas as pd
import numpy as np
import sqlite3

path = '/Users/ltakeuchi/wrds_2015/'
path_out = '/Users/ltakeuchi/Python/wrds_data/'

df = pd.read_csv(path + 'ccmfunda_annual_1950_2015.csv')
#df = pd.read_csv(path + 'ccmfunda_annual_testset.csv')
print('csv filed loaded')

######################################
# Convert dates to formatted strings #
######################################

print('converting dates')

# check for 'E'
df.ix[df.LINKENDDT=='E', 'LINKENDDT'] = np.NaN

for col in ['datadate', 'LINKDT', 'LINKENDDT', 'ipodate']:
    df[col] =  pd.to_datetime(df[col], format='%Y%m%d', errors='raise')
    print('converting ' + col)


######################################
# Check for valid link and record    #
######################################

df['endfyr'] = df['datadate']
df['begfyr'] = df['endfyr'] - pd.offsets.MonthBegin(12)

# Require DATAFMT='STD' and INDFMT='INDL' and CONSOL='C' and POPSRC='D' to 
# retrieve the standardized (as opposed to re-stated data), consolidated (as 
# opposed to pro-forma) data presented in the industrial format (as opposed to
# financial services format) for domestic companys (as opposed to international
# firms), i.e., the U.S. and Canadian firms.

filter1 = (df['indfmt'] == 'INDL') & (df['datafmt'] == 'STD') & \
          (df['popsrc'] =='D') & (df['consol'] == 'C')

# Note: wrds query already ensures filter1 will be satisfied, but verify this
if any(filter1==False):
    print('Unexpected bad record')
    

filter2 = (df['LINKTYPE'] == 'LC') | (df['LINKTYPE'] == 'LU')
filter3 = (df['LINKDT'] <= df['endfyr']) & ((df['endfyr'] <= df['LINKENDDT']) \
           | (df['LINKENDDT'].isnull()))

df = df[filter1 & filter2 & filter3]


#######################################
# Convert datetime field to string    #
#######################################

# saves space in sqlite3 database

for col in ['datadate', 'LINKDT', 'LINKENDDT', 'ipodate']:
    col2 = col + '2'
#    df[col2] =  df[col].apply(lambda x: x.strftime('%Y-%m-%d'))
    df[col2] = df[col].map(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else '')


################################
# Select variables to keep     #
################################
              
df1 = df[['LPERMNO', 'act', 'adjex_c', 'adjex_f', 'ajex', 'ajp', 'at', 'ceq',
          'ch', 'che', 'cogs', 'csho', 'cshtr_c', 'cshtr_f', 'cusip', 'dlc',
          'dltt', 'dp', 'dv', 'dvc', 'dvpsp_c', 'dvpsp_f', 'dvpsx_c',
          'dvpsx_f', 'dvsco', 'dvt', 'ebit', 'ebitda', 'endfyr', 'ib', 'itcb',
          'lct', 'lt', 'ni', 'oancf', 'pdvc', 'ppegt', 'prstkc', 'prstkcc',
          'prstkpc', 'pstk', 'pstkl', 'pstkr', 'revt', 'sale', 'seq', 'sstk',
          'txdb', 'txdi', 'txditc', 'wcap', 'xrd', 'xsga']]

###############################
# Add to sqlite3 database     #
###############################

conn = sqlite3.connect(path_out + 'crsp_compustat.sqlite')
df1.to_sql('ccma', conn, index=False)

print('added selected variables as table ccma to sqlite db')
conn.close()