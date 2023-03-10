PyMongoAgg
==========

Minimal POC of a Python-MongoDB Aggregation transpiler.

Now you can write your aggregation pipelines using native Python syntax,
and PyMongoAgg will handle conversion by parsing the AST of your function.

Example:
```python
def basic_func():
    y = a + 0
    a = (a + b) / 2
    b = (b * y) ** (1 / 2)
    t = t - (x * (y - a) ** 2)
    x = x * 2
output_dict = transpile_function(basic_func)
print(output_dict)
```
```pycon
[{'$set': {'y': {'$add': ['$a', 0]}}}, {'$set': {'a': {'$divide': [{'$add': ['$a', '$b']}, 2]}}},
 {'$set': {'b': {'$pow': [{'$multiply': ['$b', '$y']}, {'$divide': [1, 2]}]}}},
 {'$set': {'t': {'$subtract': ['$t', {'$multiply': ['$x', {'$pow': [{'$subtract': ['$y', '$a']}, 2]}]}]}}},
 {'$set': {'x': {'$multiply': ['$x', 2]}}}]
```

