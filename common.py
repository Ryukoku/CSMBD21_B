def map_func(passenger_id):
    return(passenger_id,1)

def shuffle(mapper_out):
	""" Organise the mapped values by key """
	data = {}
	for k, v in mapper_out:
		if k not in data:
			data[k] = [v]
		else:
			data[k].append(v)
	return data

def reduce_func(z):
	""" Simple sum reducer, 1 tuple arg """
	k,v=z
	return (k, sum(v))