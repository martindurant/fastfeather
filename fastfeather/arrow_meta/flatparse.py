import ast
import re

import fsspec
from .flatter import Union, Enum, namedtuple


simple_types = {"string", "byte", "int", "long", "float", "double"}
complex_types = {"union", "struct", "enum", "table"}


def parse(files: str, storage_options=None):
    objects = {}
    with fsspec.open_files(files, "rt", **(storage_options or {})) as files:
        for f in files:
            entity = False
            temp_lines = []
            for line in f:
                if len(line) < 2 or "namespace " in line or "include " in line:
                    continue
                ff = line.find("//")
                if ff > 0:
                    line = line[:ff]
                words = line.split()
                if not words:
                    continue
                if words[0] in complex_types:
                    name = words[1]
                    entity = words[0]
                    line = line[line.find("{"):]
                    temp_lines.append("".join(preformat(s) for s in line.split()))
                elif not temp_lines:
                    continue
                else:
                    if "=" in line:
                        words = line[:line.find("=")].split() + [","]  # ignore default
                    if "(required)" in line:
                        words = line[:line.find("(required)")].split() + [","]
                    temp_lines.append("".join(preformat(s) for s in words))
                if "}" in line:
                    dic_text = "\n".join(temp_lines)
                    if ":" not in dic_text:
                        # so that enums don't become unordered sets
                        dic_text = dic_text.replace("{", "[").replace("}", "]")
                    objects[name] = (entity, ast.literal_eval(dic_text))
                    temp_lines.clear()

    out = dip(objects)
    return out


def _dip(objects, name):
    if isinstance(name, list):
        return [_dip(objects, name[0])]
    if name in simple_types or name not in objects:
        return name
    val = objects[name]
    if isinstance(val, tuple) and not hasattr(val, "_asdict"):
        # not yet processed
        entity, spec = val
        if entity == "table":
            if not spec:
                return name
            out = {}
            for k, v in spec.items():
                if v == name:
                    out[k] = out
                elif v == [name]:
                    out[k] = [out]
                else:
                    out[k] = _dip(objects, v)
        elif entity == "struct":
            out = namedtuple(name, tuple(spec))(*spec.values())
        elif entity == "enum":
            out = Enum(spec)
        else:
            # union
            out = Union([_dip(objects, _) for _ in spec])
        objects[name] = out
    return objects[name]


def dip(objects):
    objects = objects.copy()
    for k in objects:
        _dip(objects, k)
    return objects


template = re.compile(r"([[\]:;,{}]?)([a-zA-Z._]*)([[\]:;,{}]*)")


def preformat(s: str) -> str:
    m = template.match(s)

    start, mid, end = m.groups()
    mid = mid.rsplit(".", 1)[-1]
    mid = f'"{mid}"' if mid else ""
    end = end.replace(";", ",")
    return "".join([start, mid, end])
