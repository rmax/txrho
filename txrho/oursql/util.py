import re


PARAM_RE = re.compile(r':(\w+)')


def named_params(sql, params=None):
    """Converts named-style parameters to qmark-style"""
    assert isinstance(sql, basestring)

    # short-circuit non-named sql
    if ':' not in sql:
        # if params is a dict underlying driver fail so don't bother to
        # check for dict
        return sql, params

    # sql seems to have named parameters and a dict params is required
    if not isinstance(params, dict):
        raise TypeError("named parameter found "
                        "but params is not a dict: {0!r}".format(params))

    # @@@ manual parsing may be faster?
    names = PARAM_RE.findall(sql)
    values = tuple(params[n] for n in names)

    # convert named to qmark
    for name in names:
        sql = sql.replace(':'+name, '?')

    # named token should have gone
    if ':' in sql:
        raise ValueError("missing named token found: "
                         "({0!r}, {1!r})".format(sql, params))

    return sql, values


