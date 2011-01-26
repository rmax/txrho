#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import stat
import sys

from collections import defaultdict
from itertools import imap

from twisted.internet import defer, reactor
from twisted.python import log
from cyclone import escape
from cyclone.web import (
    Application as _Application,
    RequestHandler as _RequestHandler,
    UIModule as _UIModule,
    _utf8,
)

from .template import Loader
from .util.defer import parallel


UI_PROCESS_CONCURRENCY = 2


class Application(_Application):
    """
    Customized Application Class
    """
    pass


class RequestHandler(_RequestHandler):
    """
    Customized RequestHandler:
        - static_url uses os.stat
        - render_later returns deferred to render template
        - render_string_deferred uses custom Template/Loader with
          deferred support
    """
    def render_later(self, template_name, **kwargs):
        """
        Deferred version of render
        """
        renderer = PageRenderer(self)
        d = renderer.render(template_name, **kwargs)
        d.addCallback(lambda html: self.finish(html))
        return d

    def render_string_deferred(self, template_name, **kwargs):
        """
        Deferred version of render_string
        """
        self.require_setting('template_path', 'render_string_deferred')
        template_path = self.settings['template_path']

        # cache template code
        if not getattr(RequestHandler, '_templates', None):
            RequestHandler._templates = {}
        registry = RequestHandler._templates

        if template_path not in registry:
            # defer support: use custom Loader with defer Template support
            registry[template_path] = Loader(template_path)
            # /defer support
        loader = registry[template_path]

        t = loader.load(template_name)
        args = dict(
            handler=self,
            request=self.request,
            current_user=self.current_user,
            locale=self.locale,
            _=self.locale.translate,
            static_url=self.static_url,
            xsrf_form_html=self.xsrf_form_html,
            reverse_url=self.reverse_url,
        )
        args.update(self.ui)
        args.update(kwargs)
        # generate() returns a deferred
        return t.generate(**args)

    def static_url(self, path):
        """
        Same as default cyclone's static_url but uses os.stat's modified time
        instead reading whole file.
        """
        self.require_setting('static_path', 'static_url')

        if not hasattr(RequestHandler, '_static_hashes'):
            RequestHandler._static_hashes = {}
        hashes = RequestHandler._static_hashes

        if path not in hashes:
            try:
                st_result = os.stat(os.path.join(self.settings.static_path, path))
            except:
                log.err("Could not open static file {0!r}".format(path))
                hashes[path] = None
            else:
                hashes[path] = str(st_result[stat.ST_MTIME])

        prefix = self.settings.get('static_url_prefix', '/static/')
        if getattr(self, 'include_host', False):
            base = '{r.protocol}://{r.host}{prefix}'.format(r=self.request, prefix=prefix)
        else:
            base = prefix

        v = hashes.get(path)
        if v:
            return '{0}{1}?v={2}'.format(base, path, v)
        else:
            return '{0}{1}'.format(base, path)

    def log(self, stuff, why='', isError=False, **kwargs):
        why = "{0} -- {1}".format(why, self._request_summary())
        if isError:
            log.err(stuff, why, **kwargs)
        else:
            log.msg(stuff, why, **kwargs)

class PageRenderer(object):
    """
    Deferred template renderer
    """
    def __init__(self, handler):
        self.handler = handler

    def render(self, template_name, **kwargs):
        d = self.handler.render_string_deferred(template_name, **kwargs)
        d.addCallback(self.process_html)
        return d

    def process_html(self, html):
        """
        Process active modules and insert embedded js/css/head
        """
        embeds = defaultdict(list)
        modules = getattr(self.handler, '_active_modules', {}).itervalues()
        concurrency = self.handler.settings.get('ui_process_concurrency', UI_PROCESS_CONCURRENCY)

        d = parallel(modules, concurrency, self.process_module, embeds)
        d.addCallback(self.insert_embeds, html, embeds)

        return d

    def process_module(self, module, embeds):
        """
        Aggregates embedded js/css files/strings
        """
        part = module.html_head()
        if part:
            embeds['heads'].append(_utf8(part))

        part = module.css_files()
        if part:
            if isinstance(part, basestring):
                embeds['css_files'].append(part)
            else: # list
                embeds['css_files'].extend(part)

        part = module.embedded_css()
        if part:
            embeds['css_inline'].append(_utf8(part))

        part = module.javascript_files()
        if part:
            if isinstance(part, basestring):
                embeds['js_files'].append(part)
            else: # list
                embeds['js_files'].extend(part)

        part = module.embedded_javascript()
        if part:
            embeds['js_inline'].append(_utf8(part))

    def insert_embeds(self, _, html, embeds):
        """
        Inserts embedded stuff into the html
        """
        top = []
        bottom = []
        unique = defaultdict(set)

        if embeds['css_files']:
            link = '<link href="{0}" type="text/css" rel="stylesheet" />'
            for path in imap(self.static_url, embeds['css_files']):
                if path not in unique['css_files']:
                    unique['css_files'].add(path)
                    top.append(link.format(escape.xhtml_escape(path)))

        if embeds['css_inline']:
            style = '<style type="text/css">\n{0}\n</style>'
            top.append(style.format('\n'.join(embeds['css_inline'])))

        if embeds['heads']:
            top.extend(embeds['heads'])

        if embeds['js_files']:
            script = '<script src="{0}" type="text/javascript"></script>'
            for path in imap(self.static_url, embeds['js_files']):
                if path not in unique['js_files']:
                    unique['js_files'].add(path)
                    bottom.append(script.format(escape.xhtml_escape(path)))

        if embeds['js_inline']:
            script = '<script type="text/javascript">\n//<![CDATA[\n{0}\n//]]>\n</script>'
            bottom.append(script.format('\n'.join(embeds['js_inline'])))

        parts = []
        if top:
            sloc = html.index('</head>')
            parts.append(html[:sloc])
            parts.extend(top)
            parts.append(html[sloc:])

        if bottom:
            if parts:
                # use last part
                fragment = parts.pop()
            else:
                fragment = html

            sloc = fragment.rindex('</body>')
            parts.append(fragment[:sloc])
            parts.extend(bottom)
            parts.append(fragment[sloc:])

        if parts:
            return '\n'.join(parts)
        else:
            return html

    def static_url(self, path):
        """
        Returns path as static url if is not absolute uri
        """
        if not path.startswith('/') and not path.startswith('http:'):
            return self.handler.static_url(path)
        return path


class UIModule(_UIModule):
    """
    UI Modules supports deferred in render method directly.
    You can render deferred templates using render_later method.

    class SomeModule(UIModule(:
        def render(self):
            # do something and get d = Deferred()
            # d = self.render_later("foo.html")
            return d

    In template:
        {% yield modules.SomeModule() %}
    """
    def render_later(self, path, **kwargs):
        """
        Renders given template path in deferred way
        """
        return self.handler.render_string_deferred(path, **kwargs)
