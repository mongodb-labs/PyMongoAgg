# Copyright (c) 2023 MongoDB
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import ast, inspect


class PipelineObject:
    def __init__(self, name, operation=None, children=None, constant=False):
        self.name = name
        self.op = operation
        self.children = children
        self.constant = constant

    @staticmethod
    def get_name(obj):
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
        if isinstance(obj, ast.Call):
            l = [PipelineObject.get_name(i) for i in obj.args]
            if len(l) == 1:
                l = l[0]
            return {f"${obj.func.id}": l}
        return obj.value

    def doc(self):
        if not self.children:
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
    ast.USub: "$subtract",
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
                None,
                operation=ops_map[node.op.__class__],
                children=[self.visit_BinOp(i) for i in node.values],
            )
        if isinstance(node, ast.UnaryOp):
            print(node.op)
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
        return PipelineObject(
            None,
            operation=f"${node.func.id}",
            children=list(map(self.visit_BinOp, node.args)),
        )

    def visit_AugAssign(self, node):
        return
        pipelines = self.visit_BinOp(node.value)
        print(node.op, node.value, node.target)
        if isinstance(pipelines, PipelineObject):
            self.objects.append(
                PipelineObject(
                    node.target.id,
                    operation=node.op,
                    children=[pipelines],
                )
            )
        else:
            self.objects.append(
                PipelineObject(
                    node.target.id,
                    operation=node.op,
                    children=[self.visit_BinOp(n) for n in pipelines],
                )
            )


def transpile_function(func):
    mapper = AggregationMapper()
    mapper.generic_visit(ast.parse(inspect.getsource(func)))
    pipeline = [i.doc() for i in mapper.objects]
    return pipeline
