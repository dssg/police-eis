#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
#=============================================================================
#
#     FileName: flask_util_js.py
#         Desc: provide flask_util.js
#               在 app.config 中可以配置:
#                   FLASK_UTIL_JS_PATH: flask_util.js 的url路径
#                   FLASK_UTIL_JS_ENDPOINT: flask_util.js 的endpoint
#
#       Author: dantezhu
#        Email: zny2008@gmail.com
#     HomePage: http://www.vimer.cn
#
#      Created: 2012-07-09 17:23:51
#      History:
#               0.0.1 | dantezhu | 2012-07-09 17:23:51 | initialization
#               0.1   | dantezhu | 2012-08-30 22:54:33 | 正式版本
#               0.2.0 | dantezhu | 2012-10-22 21:53:14 | 优化为实例的方式
#               0.2.3 | dantezhu | 2012-11-20 11:13:22 | 增加no cache
#               0.2.4 | dantezhu | 2012-11-30 10:58:13 | content-type
#               0.2.5 | dantezhu | 2012-12-04 11:41:15 | defaults不需要，缺少params报异常
#               0.2.6 | dantezhu | 2013-07-15 16:44:12 | fix bug，当params中有为0的参数时，不正常
#               0.2.7 | dantezhu | 2013-07-16 01:28:20 | 增加js直接渲染
#               0.2.8 | dantezhu | 2013-07-16 12:04:23 | 使用encodeURIComponent，否则中文有问题
#               0.2.9 | dantezhu | 2013-07-16 12:04:23 | 用tojson，支持直接放到html中
#               0.2.10 | dantezhu | 2013-07-19 11:10:41 | 必要的时候抛出异常
#               0.2.11 | dantezhu | 2013-07-21 01:45:15 | 没有必要存储_app，用flask-testing时会报错
#               0.2.13 | dantezhu | 2013-12-09 16:09:08 | 增加默认的template inject，名字为: flask_util_js
#               0.2.18 | dantezhu | 2013-12-14 20:24:50 | 优化匹配顺序
#               0.2.19 | dantezhu | 2013-12-14 20:50:43 | 不用tojson，否则看起来不方便。而且js是可以用'的
#               0.2.20 | dantezhu | 2013-12-28 19:33:10 | 修改了一些名字
#
#=============================================================================
"""

__version__ = '0.2.25'

from flask import Response, Markup
from flask import current_app
from flask import render_template_string

FLASK_UTIL_JS_PATH = '/flask_util.js'

FLASK_UTIL_JS_TPL_STRING = '''
{%- autoescape false -%}
var flask_util = function() {
    var rule_map = {{ rule_map }};

    function url_for(endpoint, params) {
        if (!params) {
            params = {};
        }

        if (!rule_map[endpoint]) {
            throw('endpoint does not exist: ' + endpoint);
        }

        var rule = rule_map[endpoint];

        var used_params = {};


        var rex = /\<\s*(\w+:)*(\w+)\s*\>/ig;

        var path = rule.replace(rex, function(_i, _0, _1) {
            if (params.hasOwnProperty(_1)) {
                used_params[_1] = params[_1];
                return encodeURIComponent(params[_1]);
            } else {
                throw(_1 + ' does not exist in params');
            }
        });

        var query_string = '';

        for(var k in params) {
            if (used_params.hasOwnProperty(k)) {
                continue;
            }

            var v = params[k];
            if(query_string.length > 0) {
                query_string += '&';
            }
            query_string += encodeURIComponent(k)+'='+encodeURIComponent(v);
        }

        var url = path;
        if (query_string.length > 0) {
            url += '?'+query_string;
        }

        return url;
    }

    return {
        url_for: url_for,
        rule_map: rule_map
    }
}();
{%- endautoescape -%}
'''

class FlaskUtilJs(object):
    """FlaskUtilJs"""

    def __init__(self, app=None):
        """init with app

        :app: Flask instance

        """
        if app:
            self.init_app(app)

    def init_app(self, app):
        """
        安装到app上
        """
        path = app.config.get('FLASK_UTIL_JS_PATH', FLASK_UTIL_JS_PATH)
        endpoint = app.config.get('FLASK_UTIL_JS_ENDPOINT', None)

        @app.route(path, endpoint=endpoint)
        def flask_util_js():
            return Response(
                self.content,
                content_type='text/javascript; charset=UTF-8',
                headers={
                    'Cache-Control':'no-cache',
                }
            )

        @app.context_processor
        def inject_fujs():
            return dict(flask_util_js=self)

        # 最后把数据写到实例里
        self._path = path
        self._endpoint = endpoint or flask_util_js.__name__

    @property
    def path(self):
        return self._path

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def content(self):
        rule_map = dict()

        for rule in current_app.url_map._rules:
            if rule.endpoint not in rule_map:
                rule_map[rule.endpoint] = rule.rule

        data = render_template_string(
            FLASK_UTIL_JS_TPL_STRING,
            rule_map=rule_map,
        )

        return data

    @property
    def js(self):
        return Markup('<script src="%s" type="text/javascript" charset="utf-8"></script>' % self.path)

    @property
    def embed_js(self):
        return Markup('<script type="text/javascript" charset="utf-8">\n%s\n</script>' % self.content)
