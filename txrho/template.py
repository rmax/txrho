"""
Deferred support in templates.

    - Template.generate returns a deferred
    - `defer` module is available in template context.
      Required for deferred support in generated code.
    - New template block {% yield <expression> %}
      where <expression> can be a deferred

Most of the code is duplicated to avoid modifying cyclone.template directly.
"""
import datetime
import os
# explicit full path import for subclassing without collision
import cyclone.template

from twisted.python import log
from twisted.internet import defer
# everything needed without modification
from cyclone import escape
from cyclone.template import (
    ParseError,
    _ApplyBlock,
    _ChunkList,
    _ControlBlock,
    _Expression,
    _ExtendsBlock,
    _IncludeBlock,
    _IntermediateControlBlock,
    _NamedBlock,
    _Statement,
    _TemplateReader,
    _Text,
    _format_code,
)


class Template(cyclone.template.Template):
    """
    Like Template but supports deferreds through inlineCallbacks decorator
    """
    def __init__(self, template_string, name="<string>", loader=None,
                 compress_whitespace=None):
        self.name = name
        if compress_whitespace is None:
            compress_whitespace = name.endswith(".html") or \
                name.endswith(".js")
        reader = _TemplateReader(name, template_string)
        # defer support: use custom file writer and parser
        self.file = _File(_parse(reader))
        # /defer support
        self.code = self._generate_python(loader, compress_whitespace)
        try:
            self.compiled = compile(self.code, self.name, "exec")
        except Exception as e:
            self._log_error(e)
            raise

    def generate(self, **kwargs):
        """
        Generate this template with the given arguments.
        Returns a deferred.
        """
        namespace = {
            "escape": escape.xhtml_escape,
            "url_escape": escape.url_escape,
            "json_encode": escape.json_encode,
            "squeeze": escape.squeeze,
            "datetime": datetime,
            # defer support: available for generated code
            "defer": defer,
            # /defer support
        }
        assert "defer" not in kwargs, "defer module override not allowed"
        namespace.update(kwargs)
        exec self.compiled in namespace
        execute = namespace["_execute"]
        d = defer.maybeDeferred(execute)
        d.addCallbacks(defer.passthru, self._log_error)
        return d

    def _log_error(self, _):
        formatted_code = _format_code(self.code).rstrip()
        log.err("%s code:\n%s" % (self.name, formatted_code))
        return _


class Loader(cyclone.template.Loader):
    """
    Like Loader but uses custom Template class
    """
    def load(self, name, parent_path=None):
        if parent_path and not parent_path.startswith("<") and \
           not parent_path.startswith("/") and \
           not name.startswith("/"):
            current_path = os.path.join(self.root, parent_path)
            file_dir = os.path.dirname(os.path.abspath(current_path))
            relative_path = os.path.abspath(os.path.join(file_dir, name))
            if relative_path.startswith(self.root):
                name = relative_path[len(self.root) + 1:]
        if name not in self.templates:
            path = os.path.join(self.root, name)
            # defer support: use custom Template
            with open(path, "r") as f:
                self.templates[name] = Template(f.read(), name=name, loader=self)
            # /defer support
        return self.templates[name]


class _File(cyclone.template._File):
    """
    Adds supports for deferred inline callbacks
    """
    def generate(self, writer):
        writer.write_line("def _execute():")
        with writer.indent():
            writer.write_line("_buffer = []")
            self.body.generate(writer)
            # defer support: use defer.returnValue instead return statement
            if getattr(writer, "inline_callbacks", False):
                writer.write_line("defer.returnValue(''.join(_buffer))")
            # /defer support
            else:
                writer.write_line("return ''.join(_buffer)")

        # defer support: decorate with inlineCallbacks
        if getattr(writer, "inline_callbacks", False):
            writer.write_line("_execute = defer.inlineCallbacks(_execute)")
        # /defer support


class _YieldBlock(_Expression):
    """
    Like _Expression but adds inline_callbacks flag to the writer
    """
    def generate(self, writer):
        super(_YieldBlock, self).generate(writer)
        writer.inline_callbacks = True


def _parse(reader, in_block=None):
    body = _ChunkList([])
    while True:
        # Find next template directive
        curly = 0
        while True:
            curly = reader.find("{", curly)
            if curly == -1 or curly + 1 == reader.remaining():
                # EOF
                if in_block:
                    raise ParseError("Missing {%% end %%} block for %s" %
                                     in_block)
                body.chunks.append(_Text(reader.consume()))
                return body
            # If the first curly brace is not the start of a special token,
            # start searching from the character after it
            if reader[curly + 1] not in ("{", "%"):
                curly += 1
                continue
            break

        # Append any text before the special token
        if curly > 0:
            body.chunks.append(_Text(reader.consume(curly)))

        start_brace = reader.consume(2)
        line = reader.line

        # Expression
        if start_brace == "{{":
            end = reader.find("}}")
            if end == -1 or reader.find("\n", 0, end) != -1:
                raise ParseError("Missing end expression }} on line %d" % line)
            contents = reader.consume(end).strip()
            reader.consume(2)
            if not contents:
                raise ParseError("Empty expression on line %d" % line)
            body.chunks.append(_Expression(contents))
            continue

        # Block
        assert start_brace == "{%", start_brace
        end = reader.find("%}")
        if end == -1 or reader.find("\n", 0, end) != -1:
            raise ParseError("Missing end block %%} on line %d" % line)
        contents = reader.consume(end).strip()
        reader.consume(2)
        if not contents:
            raise ParseError("Empty block tag ({%% %%}) on line %d" % line)

        operator, space, suffix = contents.partition(" ")
        suffix = suffix.strip()

        # Intermediate ("else", "elif", etc) blocks
        intermediate_blocks = {
            "else": set(["if", "for", "while"]),
            "elif": set(["if"]),
            "except": set(["try"]),
            "finally": set(["try"]),
        }
        allowed_parents = intermediate_blocks.get(operator)
        if allowed_parents is not None:
            if not in_block:
                raise ParseError("%s outside %s block" %
                            (operator, allowed_parents))
            if in_block not in allowed_parents:
                raise ParseError("%s block cannot be attached to %s block" % (operator, in_block))
            body.chunks.append(_IntermediateControlBlock(contents))
            continue

        # End tag
        elif operator == "end":
            if not in_block:
                raise ParseError("Extra {%% end %%} block on line %d" % line)
            return body

        elif operator in ("extends", "include", "set", "import", "comment", "yield"):
            if operator == "comment":
                continue
            if operator == "extends":
                suffix = suffix.strip('"').strip("'")
                if not suffix:
                    raise ParseError("extends missing file path on line %d" % line)
                block = _ExtendsBlock(suffix)
            elif operator == "import":
                if not suffix:
                    raise ParseError("import missing statement on line %d" % line)
                block = _Statement(contents)
            elif operator == "include":
                suffix = suffix.strip('"').strip("'")
                if not suffix:
                    raise ParseError("include missing file path on line %d" % line)
                block = _IncludeBlock(suffix, reader)
            elif operator == "set":
                if not suffix:
                    raise ParseError("set missing statement on line %d" % line)
                block = _Statement(suffix)
            # defer support: through {% yield %} inline-block
            elif operator == "yield":
                if not suffix:
                    raise ParseError("yield missing statement on line %d" % line)
                if in_block and in_block == "apply":
                    raise ParseError("yield inside {% apply %} on line %d" % line)
                block = _YieldBlock(contents)
            # /defer support
            body.chunks.append(block)
            continue

        elif operator in ("apply", "block", "try", "if", "for", "while"):
            # parse inner body recursively
            block_body = _parse(reader, operator)
            if operator == "apply":
                if not suffix:
                    raise ParseError("apply missing method name on line %d" % line)
                block = _ApplyBlock(suffix, block_body)
            elif operator == "block":
                if not suffix:
                    raise ParseError("block missing name on line %d" % line)
                block = _NamedBlock(suffix, block_body)
            else:
                block = _ControlBlock(contents, block_body)
            body.chunks.append(block)
            continue

        else:
            raise ParseError("unknown operator: %r" % operator)
