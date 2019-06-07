# coding=utf-8   //这句是使用utf8编码方式方法， 可以单独加入python头使用
import dask.dataframe as dd
import dask.bag as db
import pandas as pd
import pandas.api.types as pd_types
import time

# use decorator method with parameter
def timer_para(number = 3, repeat = 3):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for i  in range(repeat):
                start = time.perf_counter()
                for _ in range(number):
                    obj=func(*args, **kwargs)
                elapsed = (time.perf_counter() - start)
                print("func[%s] Time of %s used: %.4f\n" %(func.__name__, i+1, elapsed))
            return obj
        return wrapper
    return decorator

def mem_usage(pandas_obj):
    if(isinstance(pandas_obj, pd.DataFrame)):
        usage_b=pandas_obj.memory_usage(deep=True).sum()
    else:
        usage_b=pandas_obj.mem_usage(deep=True)
    usage_mb=usage_b/1024**2
    return "{:03.2f} MB".format(usage_mb)

@timer_para(repeat=1, number=1)
# @timeit(repeat=3, number=1)
def read_csv(infile='g:/myjb/init/20190526/instmnt_init__6a6b8e27_1d11_4af3_8aff_d25f93cdcf22', column_dict=None):
    # time_start=time.time()
    # ds = dd.read_csv('g:/myjb/loan/20190526/loan_detail__47e2718a_accd_4e81_8d69_609e6a5cfbe7')
    # time_end=time.time()
    if(column_dict!=None):
        optimized_ds = pd.read_csv(infile, dtype=column_dict)
        print(optimized_ds.info(memory_usage='deep'))
        return optimized_ds

    ds = pd.read_csv(infile)
    print('read_csv[%s] shape[%s]' %(infile, ds.shape))
    print(ds.info(memory_usage='deep'))
    # print("mem[%.2f]MB" %(ds.memory_usage(deep=true).sum()/1024/1024))
    for dtype in ['float64', 'int64', 'object']:
        sum_usage_mb=mem_usage(ds.select_dtypes(include=[dtype]))
        print("Total memory usage for {} columns: {}".format(dtype, sum_usage_mb))
    
    # optimized_ds=ds.copy()
    ds_int=ds.select_dtypes(include=['int64'])
    converted_int=ds_int.apply(pd.to_numeric, downcast='signed')
    ds_float=ds.select_dtypes(include=['float64'])
    converted_float=ds_float.apply(pd.to_numeric, downcast='float')
    ds[converted_int.columns]=converted_int
    ds[converted_float.columns]=converted_float

    optimized_ds=pd.DataFrame()
    for col in ds.columns:
        num_unique_values=len(ds[col].unique())
        num_total_values=len(ds[col])
        if num_unique_values/num_total_values < 0.3:
            optimized_ds[col]=ds[col].astype('category')
        else:
            optimized_ds[col]=ds[col]
        # if(pd_types.is_datetime64_dtype(ds[col])):
        #     optimized_ds[col]=ds[col].apply(pd.to_numeric, downcast='signed')
        # elif(pd_types.is_float_dtype(ds[col])):
        #     optimized_ds[col]=ds[col].apply(pd.to_numeric, downcast='float')
        # elif num_unique_values/num_total_values < 0.3:
        #     optimized_ds[col]=ds[col].astype('category')
        # else:
        #     optimized_ds[col]=ds[col]
    print(optimized_ds.info(memory_usage='deep'))

    dtypes=optimized_ds.dtypes
    dtypes_col=dtypes.index
    dtypes_type=[i.name for i in dtypes.values]
    columns_types=dict(zip(dtypes_col, dtypes_type))
    print(columns_types)

    return optimized_ds

@timer_para(repeat=3, number=1)
def dask_sum(ds, sum_col='prin_bal', partitions=8):
    df=dd.from_pandas(ds, npartitions=partitions)
    # df.groupby(df.encash_amt).value.sum().compute()
    amt=df[sum_col].sum().compute(scheduler="threading")/100
    print("rows[%d], amt[%.2f]" %(len(df), amt))

@timer_para(repeat=3, number=1)
def pandas_sum(ds, sum_col='prin_bal'):
    df = ds
    # df.groupby(df.encash_amt).value.sum().compute()
    amt=df[sum_col].sum()/100
    print("rows[%d], amt[%.2f]" %(df.shape[0], amt))

if __name__ == "__main__":
    columns_types={'contract_no': 'category', 'settle_date': 'category', 'term_no': 'category', 
        'start_date': 'category', 'end_date': 'category', 'status': 'category', 'clear_date': 'category', 
        'prin_ovd_date': 'category', 'int_ovd_date': 'category', 'prin_ovd_days': 'category', 
        'int_ovd_days': 'category', 
        'prin_bal': 'int32', 'int_bal': 'int32', 'ovd_prin_pnlt_bal': 'int32', 'ovd_int_pnlt_bal': 'int32'}

    ds=read_csv(infile='g:/myjb/init/20190526/instmnt_init__6a6b8e27_1d11_4af3_8aff_d25f93cdcf22', column_dict=columns_types)
    dask_sum(ds, partitions=16)
    pandas_sum(ds)