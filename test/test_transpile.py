from pymongoagg import transpile_function
from math import sqrt, log2, ceil
from pymongo import MongoClient
from bson import Decimal128
from bson.decimal128 import create_decimal128_context
from decimal import localcontext, Decimal as StdDeci, getcontext


def basic_func(a, b, t, x, y):
    y = a
    a = (a + b) / 2
    b = sqrt(b * y)
    t = t - (x * (y - a) ** 2)
    x = x * 2
    return a, b, t, x


output_dict = transpile_function(basic_func)
print(output_dict)

coll = MongoClient().db.coll
coll.drop()


def Dec(x):
    with localcontext(create_decimal128_context()) as ctx:
        return Decimal128(ctx.create_decimal(x))


dec = Dec("1.000000000000000000000000000000000000000000")
coll.insert_one(
    {
        "x": dec,
        "a": dec,
        "b": dec,
        "t": dec,
        "y": dec,
    }
)
coll.update_one(
    {},
    [
        {"$set": {"b": {"$divide": ["$b", {"$sqrt": Dec(2.0)}]}}},
        {"$set": {"t": {"$divide": ["$t", 4]}}},
    ],
)

[coll.update_one({}, output_dict) for _ in range(int(log2(34)))]
coll.update_one(
    {},
    [
        {
            "$addFields": {
                "pi": {
                    "$divide": [
                        {"$pow": [{"$add": ["$a", "$b"]}, 2]},
                        {"$multiply": [4, "$t"]},
                    ]
                }
            }
        }
    ],
)
print(l := coll.find_one({}, projection={"pi": 1, "_id": 0})["pi"])
# 3.141592653589793238462643383472675
# 3.141592653589793238462643383279502


# To make this test simpler I just calculated pi at float precision
# (Otherwise I would have to do some nasty hacks in the ast parsing
# ignore the large number of Decimal conversions I would have to do)
def exercise_basic_func():
    getcontext().prec = 34
    x = StdDeci(1)
    a = StdDeci(1)
    b = StdDeci(1 / StdDeci.sqrt(StdDeci(2)))
    t = StdDeci(1) / StdDeci(4.0)
    y = StdDeci(1)
    for _ in range(ceil(log2(34))):
        a, b, t, x = [StdDeci(i) for i in basic_func(a, b, t, x, y)]
    return StdDeci((a + b) ** StdDeci(2)) / (StdDeci(4) * t)


print((r := exercise_basic_func()))

l = l.to_decimal()
assert (l - r) < 1 / (10**17), "Pi test failed!"


def bool_func():
    y = 1
    a = (y and 0) and 1
    b = y or 0
    c = not y
    return y, a, b, c


output_dict = transpile_function(bool_func)
print(output_dict)
coll.drop()
args = ["y", "a", "b", "c"]
arg_vals = [0, 1, 0, 1]
coll.insert_one(dict(zip(args, arg_vals)))
coll.update_one({}, output_dict)
print(l := [bool(i) for i in coll.find_one({}, projection={"_id": 0}).values()])
print(r := [bool(i) for i in bool_func()])
# [True, False, True, False]
# [True, False, True, False]
assert l == r, "Boolean test failed!"
