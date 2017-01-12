import htmlmin
import csscompressor
from jsmin import jsmin
import xmlformatter


def minify_output(response, website):
    if response.headers.get('Content-Type', '').startswith('text/html'):
        response.body = htmlmin.minify(unicode(response.body), 'utf-8')
    elif response.headers.get('Content-Type', '').startswith('text/css'):
        response.body = csscompressor.compress(response.body)
    elif response.headers.get('Content-Type', '').startswith('application/javascript'):
        response.body = jsmin(response.body)
    elif response.headers.get('Content-Type', '').startswith('image/svg'):
        formatter = xmlformatter.Formatter(compress=True, indent="1")
        response.body = formatter.format_string(response.body)
