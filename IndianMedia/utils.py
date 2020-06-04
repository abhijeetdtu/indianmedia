import os

import pathlib
def getCurrentDIR():
    try:
        f = pathlib.Path(__file__).resolve().absolute()
    except:
        # Using DT virtual environment in Github/Datathon
        f = os.path.abspath(os.path.join("." ,"..","Analytics","IndianMedia","_keyfile"))

    return os.path.abspath(os.path.dirname(f))


def getDataDIR():
    dir = getCurrentDIR()
    return os.path.abspath(os.path.join(dir , "data"))


def pathToDataDIR(fname):
    return  os.path.abspath(os.path.join(getDataDIR() , fname))


def singleton(class_):
    instances = {}
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance

if __name__ == "__main__":
    print(getDataDIR())
