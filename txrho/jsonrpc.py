import types

from twisted.python import failure

from cyclone import (
    escape,
    httpclient,
)
from txrho.util.defer import (
    defer_fail,
    mustbe_deferred,
)
from txrho.web import RequestHandler


class JsonrpcHandler(RequestHandler):
    """
    JSON RPC 1.0 Service Handler
    """
    def post(self, *args):
        jsonid = None
        try:
            req = escape.json_decode(self.request.body)
            method = req["method"]
            assert isinstance(method, types.StringTypes), "{0}: {1}".format(method, type(method))
            params = req["params"]
            assert isinstance(params, (types.ListType, types.TupleType)), "{0}: {1}".format(params, type(params))
            jsonid = req["id"]
            assert isinstance(jsonid, (types.NoneType, types.IntType, types.StringTypes)), "{0}: {1}".format(jsonid, type(jsonid))
        except Exception as e:
            self.log(e, "bad request", isError=True)
            d = defer_fail(JsonrpcError("bad request"))
        else:
            # XXX: not supports nested services. e.g.: foo.bar
            function = getattr(self, "jsonrpc_{0}".format(method), None)
            if callable(function):
                args = list(args) + list(params)
                d = mustbe_deferred(function, *args)
            else:
                d = defer_fail(JsonrpcError("method not found: {0}".format(method)))

        d.addBoth(self._cbResult, jsonid)
        return d

    def _cbResult(self, result, jsonid):
        error = None
        if isinstance(result, failure.Failure):
            error = str(result.value)
            result = None

        self.finish(dict(
            result=result,
            error=error,
            jsonid=jsonid,
        ))

class JsonrpcProxy(object):
    """
    JSON RPC 1.0 Service Client
    """

    def __init__(self, endpoint, service=None):
        self.__endpoint = endpoint
        self.__service = service

    def __getattr__(self, name):
        if self.__service is not None:
            name = "{0}.{1}".format(self.__service, name)
        return self.__class__(self.__endpoint, name)

    def __call__(self, *args):
        payload = escape.json_encode(dict(
            method=self.__service,
            params=args,
            id="jsonrpc",
        ))

        d = httpclient.fetch(self.__endpoint, method="POST", postdata=payload)
        d.addCallback(self.__cbResponse)
        return d

    def __cbResponse(self, response):
        error = None
        result = None
        try:
            data = escape.json_decode(response.body)
        except ValueError:
            error = "Invalid response: {0!r}".format(response.body)
        else:
            if data["error"] is None:
                result = data["result"]
            else:
                error = data["error"]

        if error:
            return failure.Failure(JsonrpcError(error))
        else:
            return result


class JsonrpcError(Exception):
    pass
