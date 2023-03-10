PyMongoAgg
==========

Minimal POC of a Python-MongoDB Aggregation transpiler.

Now you can write your aggregation pipelines using native Python syntax,
and PyMongoAgg will handle conversion by parsing the AST of your function.

Example:
```python
def basic_func():
    y = a
    a = (a + b) / 2
    b = sqrt(b * y)
    t = t - (x * (y - a) ** 2)
    x = x * 2
output_dict = transpile_function(basic_func)
print(output_dict)
```
```pycon
[{'$set': {'y': '$a'}}, {'$set': {'a': {'$divide': [{'$add': ['$a', '$b']}, 2]}}},
 {'$set': {'b': {'$sqrt': [{'$multiply': ['$b', '$y']}]}}},
 {'$set': {'t': {'$subtract': ['$t', {'$multiply': ['$x', {'$pow': [{'$subtract': ['$y', '$a']}, 2]}]}]}}},
 {'$set': {'x': {'$multiply': ['$x', 2]}}}]
```

