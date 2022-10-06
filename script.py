
import pandas as pd
import os, sys, glob
from multiprocessing import Pool
import warnings






def load_func(files : list[str]):
    """
    Input - list of str, that contains names and dir of csv files
    Output - merged dataframe that also contains name (ID) of files
    """
    
    columns_list = ['var_symbol', 'creation_date', 'due_date', 'amount', 'VAT', 'payment_type', 'is_paid']
    
    invoices = [pd.read_csv(f, sep=';', names=columns_list) for f in files] 
    invoices = pd.concat(invoices, ignore_index=True).assign(ID = files)                # adds ID column from filename           
    invoices.ID = invoices.ID.str.replace(r"\D+", "", regex=True)                       # removes dir and .csv part from filename
    
    return invoices



def divide_into_equal_chunks(lst : list, n_chunks = 4) -> list:
    chunk_size = len(lst) // n_chunks + 1
    return [lst[i:i+chunk_size] for i in range(0, len(lst), chunk_size)]
        


def check_output_dir(output_dir : str):
    try:
        os.mkdir(output_dir)                                                            # create output dir
    except Exception as e:
        print("Output directory already exists...\n")
        
        
        
def load_data(data_dir : str, n_workers=4):
    
    all_files = glob.glob(os.path.join(data_dir, "*.csv"))                              # list
    chunks = divide_into_equal_chunks(all_files, n_chunks=n_workers)                    # dividing all the files into chunks that run in parallel

    try:
        with Pool(processes=n_workers) as p:
            invoices = p.map(load_func, chunks)
            
        data = pd.concat(invoices)                                                      # merging chunks together

        data["creation_date"] = pd.to_datetime(data["creation_date"])
        data["due_date"] = pd.to_datetime(data["due_date"])                             # for better datetime handling

        data.set_index('ID', inplace=True)
    except Exception as e:
        print("Error while loading data:", e, " aborting...\n")
        sys.exit()
        
    return data
        
        


def save_last_month_invoices(data : pd.DataFrame, filename="last_month_invoices.csv"):
    # 1. List of invoices created last month

    last_month = (pd.Timestamp.utcnow() - pd.DateOffset(months=1)).to_period("M")           # UTC instead of local, get year and month
    last_month_invoices = data[data.creation_date.dt.to_period("M")  == last_month]

    last_month_invoices.to_csv(os.path.join(output_dir, filename))                          # save to csv




def save_VAT_per_month(data : pd.DataFrame, filename="VAT_per_month.csv"):
    # 2. The total amount of created invoices per month (without VAT and with VAT)

    VAT_per_month = data.resample("M", on="creation_date")["VAT"].value_counts().sort_index().unstack('VAT').fillna(0)     #group, count VAT values, align
    VAT_per_month.index = VAT_per_month.index.to_period("M")               # don't need days

    VAT_per_month.to_csv(os.path.join(output_dir, filename))





def save_total_by_payment_type(data : pd.DataFrame, filename="total_by_payment_type.csv"):
    # 3. Total amount by Payment type

    total_by_type = data["payment_type"].value_counts().to_frame().reset_index()
    total_by_type.columns = ["Payment type", "Count"]

    total_by_type.to_csv(os.path.join(output_dir, filename), index=False)






def save_todays_unpaid_invoices(data : pd.DataFrame, filename="unpaid_today.csv"):
    # 4. Find unpaid invoices for today

    created_before_now = data.creation_date.dt.date < pd.Timestamp.utcnow().date()
    due_after_now = data.due_date.dt.date > pd.Timestamp.utcnow().date()

    unpaid_today = data[due_after_now & created_before_now & (data.is_paid == 0)]

    unpaid_today.to_csv(os.path.join(output_dir, filename))
    
    
    
    
    
    


if __name__ == '__main__':      # necessary to make multiprocessing work
            
    warnings.filterwarnings("ignore")
    
    data_dir = "inp" 
    columns_list = ['var_symbol', 'creation_date', 'due_date', 'amount', 'VAT', 'payment_type', 'is_paid']
    
    n_workers = 4
    output_dir = "output"
    
    
    check_output_dir(output_dir)
    
    print("Loading data...")
    data = load_data(data_dir, n_workers)
    print("Data loaded\n")


    try:
        save_last_month_invoices(data)
        print("List of invoices created last month - done")
        
        save_VAT_per_month(data)
        print("The total amount of created invoices per month (without VAT and with VAT) - done")
        
        save_total_by_payment_type(data)
        print("Total amount by Payment type - done")

        save_todays_unpaid_invoices(data)
        print("Find unpaid invoices for today - done\n")
    except Exception as e:
        print(e)
    
    
    print("Success")

