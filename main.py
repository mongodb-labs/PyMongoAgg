import ast, inspect


class PipelineObject:
    def __init__(self, name, operation=None, children=[], constant=False):
        self.name = name
        self.op = operation
        self.children = children
        self.constant = constant

    @staticmethod
    def get_name(obj):
        if obj is None:
            return []
        if isinstance(obj, int):
            return int(obj)
        if isinstance(obj, ast.Constant):
            return obj.value
        if isinstance(obj, PipelineObject):
            return [PipelineObject.get_name(i) for i in obj.children]

        if isinstance(obj, ast.BinOp):
            return PipelineObject.get_name(obj.left) or PipelineObject.get_name(
                obj.right
            )
        if hasattr(obj, "id"):
            return f"${obj.id}"
        if isinstance(obj, str):
            return f"${obj}"
        return obj.value

    def doc(self):
        if self.children == []:
            return self.name
        if self.constant:
            return self.name
        elif self.name and not self.op:
            children = [self.get_name(n) for n in self.children]
            if len(children) == 1 and not self.constant:
                if isinstance(self.children[0], PipelineObject):
                    return {"$set": {self.name: self.children[0].doc()}}
                return {"$set": {self.children[0]: f"${self.name}"}}
            return {"$set": {self.name: children}}
        if self.name is None:
            return {
                self.op: [
                    i.doc() if isinstance(i, PipelineObject) else self.get_name(i)
                    for i in self.children
                ]
            }
        if self.name:
            if isinstance(self.children[0], str):
                child = self.children[0]
            else:
                child = self.children[0].doc()
            return {"$set": {self.name: child}}


ops_map = {
    ast.Add: "$add",
    ast.Mult: "$multiply",
    ast.Sub: "$subtract",
    ast.Div: "$divide",
    ast.Pow: "$pow",
    ast.And: "$and",
    ast.Not: "$not",
    ast.Or: "$or",
}

testing_ops = {"Deci"}


class AggregationMapper(ast.NodeTransformer):
    def __init__(self):
        self.cur_obj = None
        self.objects = []

    def visit_BinOp(self, node):
        if isinstance(node, ast.Constant):
            return PipelineObject(node.value, None, [], constant=True)
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.BoolOp):
            for i in node.values:
                self.visit_BinOp(i)
            return PipelineObject(
                None,
                operation=ops_map[node.op.__class__],
                children=[self.visit_BinOp(i) for i in node.values],
            )
        if isinstance(node, ast.UnaryOp):
            self.visit_BinOp(node.operand)
            return PipelineObject(
                None,
                operation=ops_map[node.op.__class__],
                children=[self.visit_BinOp(node.operand)],
            )
        if isinstance(node, ast.Call):
            return self.visit_Call(node)
        if isinstance(node.left, ast.BinOp):
            node.left = self.visit_BinOp(node.left)
        if isinstance(node.right, ast.BinOp):
            node.right = self.visit_BinOp(node.right)

        return PipelineObject(
            None, operation=ops_map[node.op.__class__], children=[node.left, node.right]
        )

    def visit_Assign(self, node):
        pipelines = self.visit_BinOp(node.value)
        if isinstance(pipelines, PipelineObject):
            self.objects.append(
                PipelineObject(
                    node.targets[0].id, operation=pipelines.op, children=[pipelines]
                )
            )
        else:
            self.objects.append(
                PipelineObject(
                    pipelines,
                    operation=None,
                    children=[self.visit_BinOp(n) for n in node.targets],
                )
            )

    def visit_Call(self, node):
        if node.func.id in testing_ops:
            return
        return PipelineObject(
            None,
            operation=f"${node.func.id}",
            children=list(map(self.visit_BinOp, node.args)),
        )


def transpile_function(func):
    mapper = AggregationMapper()
    mapper.generic_visit(ast.parse(inspect.getsource(func)))
    pipeline = [i.doc() for i in mapper.objects]
    return pipeline


from math import sqrt, log2
from pymongo import MongoClient
from bson import Decimal128
from bson.decimal128 import create_decimal128_context
from decimal import localcontext, Decimal as StdDeci, setcontext


def basic_func(a, b, t, x):
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


def Deci(x):
    with localcontext() as ctx:
        ctx.prec = 34
        setcontext(ctx)
        return StdDeci(x)


dec = Dec("1.00000000000000000000000000000000000")
args = ["x", "a", "b", "t", "y"]
arg_vals = [
    dec,
    dec,
    Dec(dec.to_decimal() / StdDeci.sqrt(Dec(2.0).to_decimal())),
    Dec(dec.to_decimal() / 4),
    dec,
]
coll.insert_one(dict(zip(args, arg_vals)))

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
    x = 1.0
    a = x
    b = x / sqrt(2)
    t = x / 4
    for _ in range(4):
        a, b, t, x = basic_func(a, b, t, x)
    return ((a + b) ** 2.0) / (4.0 * t)


print((r := exercise_basic_func()))

l = float(l.to_decimal())
assert (l - r) < 1 / (10**17), "Pi test failed!"


def bool_func(y, a, b, c):
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
print(r := [bool(i) for i in bool_func(*arg_vals)])
# [True, False, True, False]
# [True, False, True, False]
assert l == r, "Boolean test failed!"
