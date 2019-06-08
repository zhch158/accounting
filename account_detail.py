# coding=utf-8   //这句是使用utf8编码方式方法， 可以单独加入python头使用
import os,sys
import argparse
import pandas as pd
import yaml

import pandas.api.types as pd_types
import memory_profiler
from utils import timer_para, get_file, load_from_yaml
import gc

from jinja2 import Environment, FileSystemLoader, Template

def mem_usage(pandas_obj):
    if(isinstance(pandas_obj, pd.DataFrame)):
        usage_b=pandas_obj.memory_usage(deep=True).sum()
    else:
        usage_b=pandas_obj.mem_usage(deep=True)
    usage_mb=usage_b/1024**2
    return "{:03.2f} MB".format(usage_mb)

@timer_para(repeat=1, number=1)
# @timeit(repeat=3, number=1)
def read_csv(infile, column_dict=None, checkfile=None):
    # time_start=time.time()
    # ds = dd.read_csv('g:/myjb/loan/20190526/loan_detail__47e2718a_accd_4e81_8d69_609e6a5cfbe7')
    # time_end=time.time()
    ret_msg=None
    ret_code=0
    if(column_dict!=None and len(column_dict)>0):
        optimized_ds = pd.read_csv(infile, dtype=column_dict)
        print('read_csv[%s] shape[%s]' %(infile, optimized_ds.shape))
        print(optimized_ds.info(memory_usage='deep'))
    else:
        ds = pd.read_csv(infile)
        print('read_csv[%s] shape[%s]' %(infile, ds.shape))
        print(ds.info(memory_usage='deep'))
        # print("mem[%.2f]MB" %(ds.memory_usage(deep=true).sum()/1024/1024))
        if(ds.shape[0]>0):
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
        else:
            optimized_ds=ds

    if(checkfile!=None):
        check_ds = pd.read_csv(checkfile)
        filename=check_ds['file_name'].values[0]
        row_count=check_ds['row_count'].values[0]
        ret_msg='{} read [%d], checkfile[row_count=%d]'.format(filename, optimized_ds.shape[0], row_count)
        if(row_count!=optimized_ds.shape[0]):
            ret_code=optimized_ds.shape[0]
    
    return ret_code, ret_msg, optimized_ds

@timer_para(repeat=3, number=1)
def pandas_sum(ds, sum_col='prin_bal'):
    df = ds
    # df.groupby(df.encash_amt).value.sum().compute()
    amt=df[sum_col].sum()/100
    print("rows[%d], amt[%.2f]" %(df.shape[0], amt))

def instmnt_init(infile, columns_types=None, check_file=None):
    ret_msg=None
    ret_code=0
    before_mem = memory_profiler.memory_usage()
    print("Memory (Before): {}Mb".format(before_mem))
    ret_code, ret_msg, ds=read_csv(infile, columns_types, check_file)
    if(ret_code!=0):
        print(ret_msg)
    after_mem = memory_profiler.memory_usage()
    print("Memory (After): {}Mb".format(after_mem))
    if(ret_code!=0):
        print(ret_msg)
    else:
        pandas_sum(ds)
    after_mem = memory_profiler.memory_usage()
    print("Memory (After): {}Mb".format(after_mem))

    del ds
    gc.collect()
    after_mem = memory_profiler.memory_usage()
    print("Memory (After): {}Mb".format(after_mem))
    
if __name__ == "__main__":
    # 测试用
    parser=argparse.ArgumentParser(description='根据配置文件，加工明细数据')
    parser.add_argument('--config', '-c', dest='inputfile', required=True, help='input config file')
    parser.add_argument('--section', '-s', dest='section', required=False, help='input section')
    parser.add_argument('--workdir', '-w', dest='workdir', required=False, help='input workdir')
    parser.add_argument('--yyyymmdd', '-d', dest='yyyymmdd', type=str, required=False, help='input date[20190101]')

    # parser.print_help()

    workdir=yyyymmdd=None

    if(len(sys.argv) == 1):
        # parser.print_help()
        args=parser.parse_args('--config ./account_detail.yaml'.split())
        # args=parser.parse_args('--yyyymmdd 20190526 --config ./account_detail.yaml --section accounting'.split())
        # args=parser.parse_args('--workdir G:/myjb --yyyymmdd 20190526 --config ./account_detail.yaml --section instmnt_init'.split())
    else:
        args=parser.parse_args()

    configfile = args.inputfile
    workdir=args.workdir
    yyyymmdd=args.yyyymmdd

    config_dic=load_from_yaml(configfile)

    config=config_dic.get("config", None)
    if(workdir==None):
        if(config!=None):
            workdir=config.get("workdir", './')
        else:
            workdir='./'
    config['workdir']=workdir
    if(yyyymmdd!=None):
        config['yyyymmdd']=yyyymmdd
    
    TemplateLoader = FileSystemLoader(searchpath=['.'])
    env = Environment(loader=TemplateLoader, variable_start_string='${', variable_end_string='}')
    md01 = env.get_template(configfile)
    content = md01.render(config)
    config_dic = yaml.load(content)

    config=config_dic.get("config", None)
    relationdatafile=config.get('relationdata', None)
    ret_code, ret_msg, relation_ds=read_csv(relationdatafile)
    if(ret_code==0):
        relation_ds['prod_code']='J1010100100000000004_3'
        relation_ds['prod_code']=relation_ds['prod_code'].astype('category')
    else:
        print(ret_msg)
        sys.exit(1)

    section_list=['accounting', 'loan_detail', 'loan_calc', 'arg_status_change', 'repay_loan_detail', 'exempt_loan_detail',  'repay_plan', 'loan_init', 'instmnt_init']
    section_name=args.section

    for section in section_list:
        if(section_name!=None):
            if(section!=section_name):
                continue
        file_dict=config_dic.get(section, None)
        if(file_dict==None):
            print("Not found [%s]" %section)
            break

        filepath=file_dict.get('path', None)
        datafile=file_dict.get('datafile', None)
        checkfile=file_dict.get('checkfile', None)
        columns_types=file_dict.get('columns_types', None)
        groupby=file_dict.get('groupby', None)
        aggregate=file_dict.get('aggregate', None)
        data_file=get_file(filepath, datafile)
        check_file=get_file(filepath, checkfile)
        ret_code, ret_msg, ds=read_csv(data_file, columns_types, check_file)
        if(ret_code!=0):
            print(ret_msg)
        # else:
        #     pandas_sum(ds)
        del ds
        gc.collect()

