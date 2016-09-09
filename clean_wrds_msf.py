# -*- coding: utf-8 -*-
"""
Clean msf (monthly CRSP) dataset from WRDS query and save as csv file
- convert dates to yyyy-mm-dd and handle missing values and special characters
- handle missing values and special characters in return fields

@author: ltakeuchi
"""

import pandas as pd

path = '/Users/ltakeuchi/wrds_2015/'
path_out = '/Users/ltakeuchi/Python/wrds_data/'

msf = pd.read_csv(path + 'msf_1925_2015.csv')
print('csv file loaded')

######################################
# Convert dates to formatted strings #
######################################

for col in ['date', 'NAMEENDT', 'DCLRDT', 'DLPDT', 'NEXTDT', 'PAYDT', 'RCRDDT',
                'SHRENDDT', 'ALTPRCDT']:
    msf[col] =  pd.to_datetime(msf[col], format='%Y%m%d', errors='raise')
    print('converting ' + col)

print('converted dates')


###########################
# Clean up numeric fields #
###########################

# returns have 'C' and NaN 

msf['RET'] = pd.to_numeric(msf['RET'], errors='coerce')
msf['RETX'] = pd.to_numeric(msf['RETX'], errors='coerce')
msf['DLRET'] = pd.to_numeric(msf['DLRET'], errors='coerce')
msf['DLRETX'] = pd.to_numeric(msf['DLRETX'], errors='coerce')

###########################
# Save as csv file        #
###########################

# date fields are converted to string with 'yyyy-mm-dd' format

msf.to_csv(path_out + 'cleaned_msf.csv')

