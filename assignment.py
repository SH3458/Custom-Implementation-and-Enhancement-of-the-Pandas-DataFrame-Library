
# - ListV2
#  - __init__
#     - self.values
#  - __add__
#  - __sub__
#  - __mul__
#  - __truediv__
#  - append
#  - mean
#  - __iter__
#  - __next__
#  - __repr___
 
# - DataFrame   
#  - __init__
#      - self.index - a dictionary to map text to row index
#      - self.data (dict of ListV2 where each column is a key)
#      - self.columns a simple list
#  - set_index
#  - __setitem__
#  - __getitem__
#  - loc       ------(select based on row index)
#  - iteritems    -----(columnwise)
#  - iterrows
#  - as_type
#  - drop       
#  - mean
#  - __repr__

class ListV2: 
    def __init__(self, values):
        self.values = values.copy()

    def __add__(self, other):
        if isinstance(other, ListV2):
            return ListV2([x + y for x, y in zip(self.values, other.values)])
        elif isinstance(other, int) or isinstance(other, float):
            return ListV2([x + other for x in self.L])
        else:
            raise ValueError("Invalid input type")
        
    def __sub__(self, other):
        if isinstance(other, ListV2):
            return ListV2([x - y for x, y in zip(self.values, other.values)])
        elif isinstance(other, int) or isinstance(other, float):
            return ListV2([x - other for x in self.L])
        else:
            raise ValueError("Invalid input type")
        
    def __mul__(self, other):
        if isinstance(other, ListV2):
            return ListV2([x * y for x, y in zip(self.values, other.values)])
        elif isinstance(other, int) or isinstance(other, float):
            return ListV2([x * other for x in self.L])
        else:
            raise ValueError("Invalid input type")
        
    def __truediv__(self, other):
        if isinstance(other, ListV2):
            return ListV2([x / y for x, y in zip(self.values, other.values)])
        elif isinstance(other, int) or isinstance(other, float):
            return ListV2([x / other for x in self.values])
        else:
            raise ValueError("Invalid input type")
        
    def append(self, value):
        self.values.append(value)      

    def mean(self):
        return sum(self.values) / len(self.values) 
       
    def __iter__(self):
        self.s = 0
        return self  
      
    def __next__(self):
        if self.s >= len(self.values):
            raise StopIteration
        else:
            self.s += 1
            return self.values[self.s - 1] 
           
    def __repr__(self):
        return "ListV2({})".format(self.values)
 



class DataFrame(ListV2):
    def __init__(self, data, index=None, columns=None):
        self.data = data
        self.index = {}
        for i in range(len(data)):
            self.index[i] = i
        self.columns = list(columns)
        self.data = {}
        for i in range(len(self.columns)):
            s = self.columns[i]
            self.data[s] = []
            for row in data:
                self.data[s].append(row[i])
        for key in self.data:
            self.data[key] = ListV2(list(self.data[key]))
    
    def __setitem__(self, key, value):
        if isinstance(key, str):
            f = False
            i = 0
            while i < len(self.columns):
                if self.columns[i] == key:
                    f = True
                    index = i
                    break
                i += 1
            if f:
                for row in self.data:
                    row[index] = value
        else: 
            self.data[key] = value
        
    def __getitem__(self, key):
        k_t = None                   #k_t = type of key
        while k_t is None:
            try:
                k_t = type(key).__name__
            except AttributeError:
                pass
        if k_t == 'str':
            return self.data[key]
        elif k_t == 'list':
            col = key
            data = [self.data[c] for c in col]
            all_r = list(zip(*data))
            output = list(all_r)
            return DataFrame(data=output, columns=col)
        elif k_t == 'tuple':
            row = key[0]
            col = key[1]
            columns = list(self.data.keys())
            try:
                columns = columns[col]
            except IndexError:
                columns = [columns[col]]
            data = []
            for col in columns:
                data.append(self.data[col])
            all_rows = list(zip(*data))
            try:
                output = all_rows[row.start:row.stop:row.step]
            except AttributeError:
                try:
                    output = [all_rows[row]]
                except IndexError:
                    raise ValueError("Invalid row index")
            return DataFrame(data=output, columns=columns)
        elif k_t == 'slice':
            x=key.start
            y=key.stop
            z=key.step
            all_r = list(zip(*self.data.values()))
            output = all_r[x:y:z]
            return DataFrame(data=output,columns=self.columns)

    def iteritems(self):
        return ((c, self.data[c].values) for c in self.columns)

    def mean(self):
        return {ele: self.data[ele].mean() for ele in self.columns}
    
    def drop(self, c):  #c = column
        del self.data[c]
        self.columns.remove(c)
    
    def set_index(self, c):
        self.index = {}
        for index in range(len(c)):
            value = c[index]
            self.index[value] = index

    def loc(self, key):
        s = []
        if type(key) == tuple:
            r, c = key      
            for ele in c:
                s.append(self.data[ele])
            new_s = DataFrame(data=list(zip(*s))[0:len(r)], columns=c)
            new_s.set_index(r)
            return new_s
                   
    def iterrows(self):
        s = list(map(lambda x: tuple(x[1]), sorted([(i, v) for i, v in enumerate(zip(*self.data.values()))])))
        c = self.index.keys()
        return list(dict(zip(c, s)).items())
    
    def as_type(self, c, t):
        self.data[c] = ListV2(list(map(t, self.data[c])))   
  
    def __repr__(self):
        r = []
        r.append(',' + ','.join(list(self.columns)))
        for index, r_v in zip(self.index.keys(), zip(*self.data.values())):  #r_v = values in the row
            row = [str(index)]
            row.extend(str(value) for value in r_v)
            r.append(','.join(row))
        return '\n'.join(r)
    

            
    





        