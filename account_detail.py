# coding=utf-8   //这句是使用utf8编码方式方法， 可以单独加入python头使用
import os,sys
import threading
import argparse

# import pandas.api.types as pd_types
# from numba import jit, autojit 
import yaml

import memory_profiler
from utils import timer_para, get_file, load_from_yaml, MyThread
import gc

from jinja2 import Environment, FileSystemLoader, Template

def mem_usage(pandas_obj):
    if(pandas_obj.empty):
        return '0.00 MB'

    if(isinstance(pandas_obj, pd.DataFrame)):
        usage_b=pandas_obj.memory_usage(deep=True).sum()
    else:
        usage_b=pandas_obj.memory_usage(deep=True)
    usage_mb=usage_b/1024**2
    return "{:03.2f} MB".format(usage_mb)

@timer_para(repeat=1, number=1)
def read_csv(infile, column_dict=None, checkfile=None):
    # time_start=time.time()
    # ds = dd.read_csv('g:/myjb/loan/20190526/loan_detail__47e2718a_accd_4e81_8d69_609e6a5cfbe7')
    # time_end=time.time()
    ret_msg=None
    ret_code=0
    if(column_dict!=None and len(column_dict)>0):
        optimized_ds = pd.read_csv(infile, dtype=column_dict)
        print('read_csv[{}] shape[{}]'.format(infile, optimized_ds.shape))
        if(optimized_ds.shape[0]!=0):
            print(optimized_ds.info(memory_usage='deep'))
    else:
        ds = pd.read_csv(infile, dtype=column_dict)
        print('read_csv[{}] shape[{}]'.format(infile, ds.shape))
        # print("mem[%.2f]MB" %(ds.memory_usage(deep=true).sum()/1024/1024))
        if(ds.shape[0]>0):
            print(ds.info(memory_usage='deep'))
            for dtype in ['float64', 'int64', 'object']:
                sum_usage_mb=mem_usage(ds.select_dtypes(include=[dtype]))
                print("Total memory usage for {} columns: {}".format(dtype, sum_usage_mb))
            
            # optimized_ds=ds.copy()
            ds_int=ds.select_dtypes(include=['int64'])
            if(ds_int.empty==False):
                converted_int=ds_int.apply(pd.to_numeric, downcast='signed')
                ds[converted_int.columns]=converted_int
            ds_float=ds.select_dtypes(include=['float64'])
            if(ds_float.empty==False):
                converted_float=ds_float.apply(pd.to_numeric, downcast='float')
                ds[converted_float.columns]=converted_float


            for i, col in enumerate(ds.columns):
                num_unique_values=len(ds[col].unique())
                num_total_values=len(ds[col])
                if(i==0):
                    if num_unique_values/num_total_values < 0.3:
                        optimized_ds=pd.DataFrame(ds[col], dtype='category')
                    else:
                        optimized_ds=pd.DataFrame(ds[col])
                else:
                    if num_unique_values/num_total_values < 0.3:
                        optimized_ds[col]=ds[col].astype('category')
                    else:
                        optimized_ds[col]=ds[col]
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
        ret_msg='{} read [{}], checkfile[row_count={}]'.format(filename, optimized_ds.shape[0], row_count)
        if(row_count!=optimized_ds.shape[0]):
            ret_code=optimized_ds.shape[0]
    
    return ret_code, ret_msg, optimized_ds

@timer_para(repeat=1, number=1)
def write_csv(ds, outdir, section):
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    o_file= os.path.join(outdir, section)
    if(ds.shape[0]>0):
        ds.to_csv(o_file, encoding='utf-8', index=False, header=True)
    else:
        f=open(o_file, mode='w', encoding='utf-8')
        row=None
        for i, col in enumerate(ds.columns.to_list()):
            if(i==0):
                row=col
            else:
                row += ',' + col
        f.write(row)
        f.close()

def set_value(row, datafile, groupby, col):
    detail_type=datafile + '.'
    for group_col in groupby:
        # detail_type += row[group_col].values[0] + '.'
        detail_type += row[group_col] + '.'
    detail_type+=col
    return detail_type

@timer_para(repeat=1, number=1)
# @autojit
def aggregate_detail(ds, relation_ds, groupby, agg_cols, glob_conf, section_conf):
    ret_msg=None
    ret_code=0
    r={'detail_type':[], 'detail_cnt':[], 'detail_amt':[]}
    if(ds.shape[0]==0):
        return ret_code, ret_msg, ds, relation_ds, pd.DataFrame(r)

    mayi_2=glob_config.get('mayi_2', None)
    mayi_3=glob_config.get('mayi_3', None)
    if('prod_code' in ds.columns):
        mayi_ds=ds[['contract_no', 'prod_code']]
        mayi_ds=mayi_ds.loc[mayi_ds['prod_code']==mayi_3]
        print("before merge shape[{}]".format(relation_ds.shape))
        relation_ds=pd.merge(relation_ds, mayi_ds, how='outer', on=['contract_no', 'prod_code'])
        relation_ds['prod_code']=relation_ds['prod_code'].astype('category')
        print("after merge shape[{}]".format(relation_ds.shape))
    else:
        # ds.insert(0, 'prod_code')
        # ds['prod_code']=ds['prod_code'].astype('object')
        # ds['prod_code']=ds['prod_code'].astype('category')
        ds=pd.merge(ds, relation_ds, how='left', on='contract_no')
        ds['prod_code']=ds['prod_code'].fillna(mayi_2).astype("category")

    datafile=section_conf.get('datafile', None)

    if(groupby==None or len(groupby)==0):
        columns=agg_cols
        gp_ds=ds[columns].agg(['count', 'sum'])
        for col in agg_cols:
            r['detail_type']+=datafile + '.' + col
            r['detail_cnt']+=gp_ds[col].loc['count'].values.tolist()
            r['detail_amt']+=gp_ds[col].loc['sum'].values.tolist()
    else:
        columns=groupby + agg_cols
        gp_ds=ds[columns].groupby(groupby).agg(['count', 'sum'])
        gp_ds=gp_ds.reset_index()
        # gp_ds=gp_ds.rename(columns={gp_ds.columns[0][0]:'detail_type'})
        for col in agg_cols:
            # out['detail_type']=gp_ds.apply(lambda row: datafile + '.' + row['detail_type'].values[0] + '.' + col, axis=1)
            # out['detail_type']=gp_ds.apply(lambda row: set_value(row, datafile, groupby, col), axis=1)
            # gp_ds_type=gp_ds.apply(set_value, axis=1, **{'datafile':datafile, 'groupby':groupby, 'col':col})
            gp_ds_type=gp_ds.apply(set_value, axis=1, datafile=datafile, groupby=groupby, col=col)
            r['detail_type']+=gp_ds_type.iloc[:,0].values.tolist()
            r['detail_cnt']+=gp_ds[col, 'count'].values.tolist()
            r['detail_amt']+=gp_ds[col, 'sum'].values.tolist()
    out_ds=pd.DataFrame({'detail_type':r['detail_type'], 'detail_cnt':r['detail_cnt'], 'detail_amt':r['detail_amt']})
    return ret_code, ret_msg, ds, relation_ds, out_ds
    
def deal_csv(section, file_dict, glob_conf, relation_ds):
    ret_code=0
    ret_msg=None

    out_ds=pd.DataFrame({'detail_type':[], 'detail_cnt':[], 'detail_amt':[]})
    print("\n\nNow begin deal section[{}]".format(section))
    filepath=file_dict.get('path', None)
    datafile=file_dict.get('datafile', None)
    checkfile=file_dict.get('checkfile', None)
    columns_types=file_dict.get('columns_types', None)
    groupby=file_dict.get('groupby', None)
    agg_cols=file_dict.get('agg_cols', None)
    action=file_dict.get('action', None)

    data_file=get_file(filepath, datafile)
    check_file=get_file(filepath, checkfile)

    ret_code, ret_msg, ds=read_csv(data_file, columns_types, check_file)
    if(ret_code!=0):
        return ret_code, ret_msg, relation_ds, out_ds
        # raise Exception("section[{}] read_csv error[{}]".format(section, ret_msg))  
    else:
        if(action!=None and action!=''):
            func=globals().get(action)
            if(func!=None):
                ret_code, ret_msg, ds, relation_ds, out_ds=func(ds, relation_ds, groupby, agg_cols, glob_config, file_dict)
                if(ret_code!=0):
                    return ret_code, ret_msg, relation_ds, out_ds
                    # raise Exception("section[{}] func[{}] error[{}]".format(section, action, ret_msg))  
            else:
                print("\ndatafile[{}] find function[{}] error".format(datafile, action))
    
    outdir=glob_conf.get('outdir', '.')
    write_csv(ds, outdir, datafile+'.csv')
    del ds
    gc.collect()
    return ret_code, ret_msg, relation_ds, out_ds

if __name__ == "__main__":
    # 测试用
    parser=argparse.ArgumentParser(description='根据配置文件，加工明细数据')
    parser.add_argument('--config', '-c', dest='inputfile', required=True, help='input config file')
    parser.add_argument('--section', '-s', dest='section', required=False, help='input section')
    parser.add_argument('--workdir', '-w', dest='workdir', required=False, help='input workdir')
    parser.add_argument('--yyyymmdd', '-d', dest='yyyymmdd', type=str, required=False, help='input date[20190101]')
    parser.add_argument('--lib', '-l', dest='lib', type=str, required=False, help='input lib[pandas|modin]', choices=['pandas', 'modin'])

    # parser.print_help()

    workdir=yyyymmdd=None

    if(len(sys.argv) == 1):
        # parser.print_help()
        # args=parser.parse_args('--config ./account_detail.yaml'.split())
        args=parser.parse_args('--yyyymmdd 20190525 --config ./account_detail.yaml --section exempt_loan_detail'.split())
        # args=parser.parse_args('--workdir G:/myjb --yyyymmdd 20190525 --config ./account_detail.yaml --section arg_status_change'.split())
    else:
        args=parser.parse_args()

    configfile = args.inputfile
    workdir=args.workdir
    yyyymmdd=args.yyyymmdd
    lib=args.lib

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
    else:
        yyyymmdd=config.get("yyyymmdd", '20190101')
    if(lib!=None):
        config['lib']=lib
    else:
        lib=config.get("lib", 'pandas')
    
    
    TemplateLoader = FileSystemLoader(searchpath=['.'])
    env = Environment(loader=TemplateLoader, variable_start_string='${', variable_end_string='}')
    md01 = env.get_template(configfile)
    content = md01.render(config)
    config_dic = yaml.load(content)

    glob_config=config_dic.get("config", None)
    outdir=glob_config.get('outdir', '.')

    if(lib=='pandas'):
        import pandas as pd
        from pandas.api.types import CategoricalDtype
    elif(lib=='modin'):
        import modin.pandas as pd
        from modin.pandas import CategoricalDtype
    else:
        raise Exception("lib name[{}] error".format(lib))  

    relationdatafile=glob_config.get('relationdata', None)
    mayi_2=glob_config.get('mayi_2', None)
    mayi_3=glob_config.get('mayi_3', None)
    mayi_type = CategoricalDtype(categories=[mayi_2, mayi_3])

    ret_code, ret_msg, ds=read_csv(relationdatafile)
    if(ret_code==0):
        ds['prod_code']=mayi_3
        relation_ds=ds.copy()
        relation_ds['prod_code']=relation_ds['prod_code'].astype(mayi_type)
    else:
        raise Exception("section[{}] read_csv error[{}]".format('relationdata', ret_msg))  

    section_list_single=['accounting', 'loan_detail']
    section_list_multiple=['loan_calc', 'arg_status_change', 'exempt_loan_detail', 'repay_loan_detail',  'repay_plan', 'loan_init']

    # section_list_single+=section_list_multiple
    # section_list_multiple=[]

    section_name=args.section

    out_ds_list={}
    for section in section_list_single:
        if(section_name!=None):
            if(section!=section_name):
                continue
        file_dict=config_dic.get(section, None)
        if(file_dict==None):
            raise Exception("section[{}] not found".format(section))  

        ret_code, ret_msg, relation_ds, out_ds_list[section] =deal_csv(section, file_dict, glob_config, relation_ds)
        if(ret_code!=0):
            raise Exception("section[{}] func[{}] error[{}]".format(section, 'deal_csv', ret_msg))  

    threads=[]
    threads_num=0
    for section in section_list_multiple:
        if(section_name!=None):
            if(section!=section_name):
                continue
        file_dict=config_dic.get(section, None)
        if(file_dict==None):
            raise Exception("section[{}] not found".format(section))  

        t=MyThread(deal_csv,args=(section, file_dict, glob_config, relation_ds))
        threads.append(t)
        t.start()
        threads_num+=1
        # print("\n\nNow begin deal section[{}]".format(section))
        # ret_code, ret_msg, relation_ds, out_ds =deal_csv(file_dict, glob_config, relation_ds, out_ds)
        # if(ret_code!=0):
        #     raise Exception("section[{}] func[{}] error[{}]".format(section, 'deal_csv', ret_msg))  

    for t in threads:
        t.join()  # 一定要join，不然主线程比子线程跑的快，会拿不到结果
        ret_code, ret_msg, relation_ds, out_ds_list[t.args[0]] = t.get_result()
        if(ret_code!=0):
            raise Exception("thread[{}] func[{}] error[{}]".format(t, 'deal_csv', ret_msg))  

    # out_ds=pd.DataFrame({'openday':[], 'detail_type':[], 'detail_cnt':[], 'detail_amt':[]})
    i=0
    out_ds=pd.DataFrame({'openday':[], 'detail_type':[], 'detail_cnt':[], 'detail_amt':[]})
    for key, ds in out_ds_list.items():
        if(ds.shape[0]>0):
            if(i==0):
                out_ds=ds.copy()
            else:
                out_ds=out_ds.append(ds)
            i+=1
    if(out_ds.shape[0]>0):
        out_ds.insert(0, 'openday', yyyymmdd)
    print(out_ds)

    write_csv(out_ds, outdir, 'detail_stat.' + yyyymmdd + '.csv')
    write_csv(relation_ds, outdir, 'contract_no_3.' + yyyymmdd + '.csv')
    sys.exit(0)
