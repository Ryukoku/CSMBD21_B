import multiprocessing as mp
import math
import pandas as pd
from common import map_func, reduce_func as reduce_func, shuffle

if __name__ == '__main__':
	fileName = 'AComp_Passenger_data_no_error.csv'
	df = pd.read_csv(fileName,
					 names=["passenger_id", "flight_id", "departure_code", "arrive_code", "departure_time",
							"flight_time"])

	map_in = df['passenger_id']
	with mp.Pool(processes=mp.cpu_count()) as pool:
		map_out = (pool.map(map_func, map_in, chunksize= math.ceil(len(map_in)/mp.cpu_count())))
		reduce_in = shuffle(map_out)
		reduce_out = pool.map(reduce_func, reduce_in.items(), chunksize=math.ceil(len(reduce_in.keys())/mp.cpu_count()))
		max_flight = max(reduce_out,key=lambda item:item[1])
		print('Run with', mp.cpu_count(), 'cpus')
		print('Most Flight Passenger:', max_flight[0]+'with total of:', max_flight[1] ,'flights')


