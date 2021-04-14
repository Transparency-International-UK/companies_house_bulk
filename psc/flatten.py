def iteritems_nested(dict_):
	"""
	factory func to contain function "fetch" returning list of tuples
	[(list of suffixes, value), ..., n] where n is len(dict_.keys()).

	usage:
	>>> from psc.flatten import iteritems_nested
	... d = {"a":1,
	...      "b": 2,
	...      "c": {"aa" : 3},
	...      "d" : ["cant be unpacked"]}
	... generator = iteritems_nested(dict_=d)
	... l = list(generator)
	... print(l)
	[(['d'], ['cant be unpacked']), (['a'], 1), (['c', 'aa'], 3), (['b'], 2)]
	"""

	def fetch(suffixes, v0):
		"""
		generator func unpacking the iterable dict_ recursively until all
		dict_ dictionaries are un-nested or dict_ element is not a dictionary.
		:param suffixes: list, the keys of the dictionary.
		Assigned empty list as argument by factory func return stmt.
		Empty list iteratively incremented with keys of the dictionary in
		the inner for loop.
		:param v0: variable to be unpacked. Assigned dict_ as argument when
		called by factory func return stmt.
		:return: generator object which will be evaluated in the
		"flatten_dict_not_lists" function.
		"""
		if isinstance(v0, dict):
			for k, v in v0.items():
				for i in fetch(suffixes + [k], v):
					# yield tuple (list of suffixes, variable which could
					# be further unpacked)
					yield i
		else:
			# yield tuple (list of suffixes, variable which cannot
			# be further unpacked)
			yield suffixes, v0

	return fetch([], dict_)


def flatten_nested_dicts_only(dict_, drop=None):
	"""
	func creating new flattened dictionary from the generator
	iteritems_nested(dict_).
	usage:
	>>> from pprint import pprint
	... from psc.flatten import flatten_nested_dicts_only as flatten
	... d = {"a": 1, "b": 2, "c": {"aa": 3}, "d": ["cant be unpacked"]}
	... pprint(flatten(dict_=d, drop=["d"]))
	{'a': 1, 'b': 2, 'c_aa': 3}
	"""
	if drop is None:
		drop = []

	# k[0] is the root level key of the json. User can decide to recompose a
	# flattened JSON skipping certain keys.
	return dict(('_'.join(ks), v) for ks, v in iteritems_nested(dict_)
				if ks[0] not in drop)
