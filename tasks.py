import gevent
from gevent import monkey; monkey.patch_all()
import socket
hosts = ['www.crappytaxidermy.com', 'www.walterpottertaxidermy.com', 'www.antique-taxidermy.com']
jobs = [gevent.spawn(socket.gethostbyname, host) for host in hosts]
gevent.joinall(jobs, timeout=5)
for job in jobs:
    print(job.value)



# import gevent
# from gevent import socket
# hosts = ['www.crappytaxidermy.com', 'www.walterpottertaxidermy.com',  'www.antique-taxidermy.com']
# jobs = [gevent.spawn(gevent.socket.gethostbyname, host) for host in hosts]
# gevent.joinall(jobs, timeout=1)
# for job in jobs:
#     print(job.value)


# from concurrent import futures
# import math, time
# import sys

# def calc(val):
#     result = math.sqrt(float(val))
#     time.sleep(1)
#     return val, result

# def use_threads(num, values):
#     with futures.ThreadPoolExecutor(num) as tex:
#         tasks = [tex.submit(calc, value) for value in values]
#         for f in futures.as_completed(tasks):
#             print('=thread=' + str(f.result()))
#             yield f.result()

# def use_processes(num, values):
#     with futures.ProcessPoolExecutor(num) as pex:
#         tasks = [pex.submit(calc, value) for value in values]
#         for f in futures.as_completed(tasks):
#             print('=process=' + str(f.result()))
#             yield f.result()

# def main(workers, values):
#     print(f"Using {workers} workers for {len(values)} values")
#     print("Using threads:")
#     for val, result in use_threads(workers, values):
#         print(f'{val} {result:.4f}')
#     print("Using processes:")
#     for val, result in use_processes(workers, values):
#         print(f'{val} {result:.4f}')

# if __name__ == '__main__':
#     workers = 3
#     if len(sys.argv) > 1:
#         workers = int(sys.argv[1])
#     values = list(range(1, 6)) # 1 .. 5
#     main(workers, values)




# from concurrent import futures
# import math
# import time
# import sys

# def calc(val):
#     time.sleep(0.3)
#     result = math.sqrt(float(val))
#     return result

# def use_threads(num, values):
#     t1 = time.time()
#     with futures.ThreadPoolExecutor(num) as tex:
#         results = tex.map(calc, values)
#         print(list(results))
#     t2 = time.time()
#     return t2 - t1

# def use_processes(num, values):
#     t1 = time.time()
#     with futures.ProcessPoolExecutor(num) as pex:
#         results = pex.map(calc, values)
#         print(list(results))
#     t2 = time.time()
#     return t2 - t1

# def main(workers, values):
#     print(f"Using {workers} workers for {len(values)} values")
#     t_sec = use_threads(workers, values)
#     print(f"Threads took {t_sec:.4f} seconds")
#     p_sec = use_processes(workers, values)
#     print(f"Processes took {p_sec:.4f} seconds")

# if __name__ == '__main__':
#     workers = int(sys.argv[1])
#     values = list(range(1, 6)) # 1 .. 5
#     main(workers, values)



# import multiprocessing as mp

# def washer(dishes, output):
#     for dish in dishes:
#         print('Washing', dish, 'dish')
#         output.put(dish)

# def dryer(input):
#     while True:
#         dish = input.get()
#         print('Drying', dish, 'dish')
#         input.task_done()

# dish_queue = mp.JoinableQueue()
# dryer_proc = mp.Process(target=dryer, args=(dish_queue,))
# dryer_proc.daemon = True
# dryer_proc.start()

# dishes = ['salad', 'bread', 'entree', 'dessert']
# washer(dishes, dish_queue)
# dish_queue.join()

# import threading, queue
# import time

# def washer(dishes, dish_queue):
#   for dish in dishes:
#       print ("Washing", dish)
#       time.sleep(1)
#       dish_queue.put(dish)

# def dryer(dish_queue):
#   while True:
#       dish = dish_queue.get()
#       print ("Drying", dish)
#       time.sleep(2)
#       dish_queue.task_done()


# dish_queue = queue.Queue()
# for n in range(2):
#   dryer_thread = threading.Thread(target=dryer, args=(dish_queue,))
#   dryer_thread.start()

# dishes = ['salad', 'bread', 'entree', 'dessert']
# washer(dishes, dish_queue)
# dish_queue.join()
