#Flattening lists: converting a 2D list into a 1D list.

import itertools
a=[[1,2],[3,4],[5,6]]
b=list(itertools.chain.from_iterable(a))
print(b)