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
        if self.children==[]:
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
                None, operation=ops_map[node.op.__class__], children=[self.visit_BinOp(i) for i in node.values]
            )
        if isinstance(node, ast.UnaryOp):
            self.visit_BinOp(node.operand)
            return PipelineObject(
                None, operation=ops_map[node.op.__class__], children=[self.visit_BinOp(node.operand)]
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
from decimal import localcontext, Decimal


def basic_func():
    y = a
    a = (a + b) / 2
    b = sqrt(b * y)
    t = t - (x * (y - a) ** 2)
    x = x * 2


output_dict = transpile_function(basic_func)
print(output_dict)

coll = MongoClient().db.coll
coll.drop()


def Dec(x):
    with localcontext(create_decimal128_context()) as ctx:
        return Decimal128(ctx.create_decimal(x))


dec = Dec("1.00000000000000000000000000000000000")
coll.insert_one(
    {
        "x": dec,
        "a": dec,
        "b": Dec(dec.to_decimal() / Decimal.sqrt(Dec(2.0).to_decimal())),
        "t": Dec(dec.to_decimal() / 4),
        "y": dec,
    }
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
print(coll.find_one({}, projection={"pi": 1, "_id": 0}))
from math import pi

print(pi)
# {'pi': Decimal128('3.141592653589793238462643383472675')}
#                    3.141592653589793238462643383279502


def bool_func():
    y = 1
    a = (y and 0) and 1
    b = y or 0
    c = not y


output_dict = transpile_function(bool_func)
print(output_dict)
coll.drop()
coll.insert_one(
    {
        "y": 0,
        "a": 1,
        "b": 0,
        "c": 1,
    }
)
coll.update_one({}, output_dict)
print(coll.find_one({}))
#{'_id': ObjectId('6410106d834a827ffb542c72'), 'y': 1, 'a': False, 'b': True, 'c': False}

