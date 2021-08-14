from multiprocessing import Process
from services.blocks_service import ETLService

#main
def SpawnSingletonETLService():
    ETLService()

if __name__ == '__main__':
    p = Process(target=SpawnSingletonETLService, args=())
    p.start()
    p.join()