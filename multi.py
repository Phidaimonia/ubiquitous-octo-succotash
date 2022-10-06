# %%
#%pip install pandas


# %%

import pandas as pd
import matplotlib.pyplot as plt
import datetime

import os
import glob
from time import monotonic
from multiprocessing import Pool



# Loading into dataframe - solution 3 - 10s


def load_func(files : list[str]):
    columns_list = ['var_symbol', 'creation_date', 'due_date', 'amount', 'VAT', 'payment_type', 'is_paid']
    
    invoices = [pd.read_csv(f, sep=';', names=columns_list) for f in files] 
    invoices = pd.concat(invoices, ignore_index=True).assign(ID = files)            # adds ID column from filename           
    invoices.ID = invoices.ID.str.replace(r"\D+", "", regex=True)  
    
    return invoices


def divide_into_equal_chunks(lst : list, n = 4) -> list:
    chunk_size = len(lst) // n + 1
    return [lst[i:i+chunk_size] for i in range(0, len(lst), chunk_size)]
        
        
            

if __name__ == '__main__':      # necessary to make multiprocessing work
        
    data_dir = "inp" 
    start_time = monotonic()

    n_workers = 4


    all_files = glob.glob(os.path.join(data_dir, "*.csv"))                          # list
    chunks = divide_into_equal_chunks(all_files, n=4)                               # dividing all the files into chunks that run in parallel

    print("files done:", (monotonic() - start_time) * 1000.0, "ms")

    with Pool(processes=n_workers) as p:
        invoices = p.map(load_func, chunks)
        
    print("processing done:", (monotonic() - start_time) * 1000.0, "ms")
    
    data = pd.concat(invoices)                                                      # merging chunks together
    
    print("merging done:", (monotonic() - start_time) * 1000.0, "ms")


    data["creation_date"] = pd.to_datetime(data["creation_date"])
    data["due_date"] = pd.to_datetime(data["due_date"])                             # for better datetime handling

    data.reset_index(drop=True, inplace=True)
    data.head()

