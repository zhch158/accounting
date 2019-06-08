# coding=utf-8   //这句是使用utf8编码方式方法， 可以单独加入python头使用
import os
import time
import memory_profiler
import yaml

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

def get_file(dir, pattern):
    data_file=None
    # 路径（鼠标右键查看文件属性）
    # path = os.path.join(dir, date)
    files = os.listdir(dir)
    # 查找文件名字以pattern开头的文件
    for f in files:
        if(f.startswith(pattern)):
            data_file=f
    if(data_file!=None):
        return os.path.join(dir, data_file)
    else:
        return None

def load_from_yaml(src):
    f = open(src, encoding='utf-8')
    x = yaml.load(f)
    f.close()

    return x

#only support UNIX
# import resource
# def using():
#     usage = resource.getrusage(resource.RUSAGE_SELF)
#     mem = usage[2]*resource.getpagesize() /1000000.0
#     print("mem: ", mem,  " Mb")
#     return mem
