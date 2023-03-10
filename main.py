import ast, inspect


class PipelineObject:
    def __init__(self, name, operation=None, children=[]):
        self.name = name
        self.op = operation
        self.children = children

    @staticmethod
    def get_name(obj):
        if isinstance(obj, PipelineObject):
            return [PipelineObject.get_name(i) for i in obj.children]
        if hasattr(obj, "id"):
            return f"${obj.id}"
        elif isinstance(obj, ast.BinOp):
            return PipelineObject.get_name(obj.left) or PipelineObject.get_name(
                obj.right
            )
        else:
            return obj.value

    def doc(self):
        if self.name is None:
            return {
                self.op: [
                    i.doc() if isinstance(i, PipelineObject) else self.get_name(i)
                    for i in self.children
                ]
            }
        return {"$set": {self.name: self.children[0].doc()}}


ops_map = {
    ast.Add: "$add",
    ast.Mult: "$multiply",
    ast.Sub: "$subtract",
    ast.Div: "$divide",
    ast.Pow: "$pow",
}


class AggregationMapper(ast.NodeTransformer):
    def __init__(self):
        self.cur_obj = None
        self.objects = []

    def visit_BinOp(self, node: ast.BinOp):
        if not isinstance(node.left, PipelineObject):
            self.visit(node.left)
        if not isinstance(node.right, PipelineObject):
            self.visit(node.right)
        if not isinstance(node.op, PipelineObject):
            self.visit(node.op)
        if isinstance(node.left, ast.BinOp):
            node.left = self.visit_BinOp(node.left)
        if isinstance(node.right, ast.BinOp):
            node.right = self.visit_BinOp(node.right)

        return PipelineObject(
            None, operation=ops_map[node.op.__class__], children=[node.left, node.right]
        )

    def visit_Assign(self, node):
        pipelines = self.visit_BinOp(node.value)
        self.objects.append(
            PipelineObject(
                node.targets[0].id, operation=pipelines.op, children=[pipelines]
            )
        )


def transpile_function(func):
    mapper = AggregationMapper()
    mapper.generic_visit(ast.parse(inspect.getsource(func)))
    pipeline = [i.doc() for i in mapper.objects]
    return pipeline


from math import sqrt, log2
from pymongo import MongoClient
from decimal import localcontext
from bson import Decimal128
from bson.decimal128 import create_decimal128_context


def basic_func():
    y = a + 0
    a = (a + b) / 2
    b = (b * y) ** (1 / 2)
    t = t - (x * (y - a) ** 2)
    x = x * 2


output_dict = transpile_function(basic_func)
print(output_dict)

coll = MongoClient().db.coll
coll.drop()


def Dec(x):
    with localcontext(create_decimal128_context()) as ctx:
        return Decimal128(ctx.create_decimal(x))


coll.insert_one(
    {
        "x": Dec(1.0),
        "a": Dec(1.0),
        "b": Dec(1.0 / sqrt(2.0)),
        "t": Dec(1.0 / 4),
        "y": Dec(1.0),
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