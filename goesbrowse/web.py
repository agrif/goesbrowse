import collections
import json

import flask
import pygments
import pygments.lexers
import pygments.formatters

import goesbrowse.config
import goesbrowse.database
import goesbrowse.pagination

app = flask.Flask(__name__)

@app.before_first_request
def setup_app():
    global app
    conf = goesbrowse.config.discover([app.config.get('configpath')])
    app.config['config'] = conf

def get_db():
    global app
    db = getattr(flask.g, 'database', None)
    if db is None:
        if not 'config' in app.config:
            setup_app()
        conf = app.config['config']
        db = flask.g.database = goesbrowse.database.Database(
            conf.files,
            conf.database,
            conf.quota,
        )
    return db

# helper to create a url to current page, with modified args
def url_for_args(**kwargs):
    args = flask.request.view_args.copy()
    for k, v in flask.request.args.items():
        args[k] = v
    for k, v in kwargs.items():
        if v is None:
            if k in args:
                del args[k]
        else:
            args[k] = v
    return flask.url_for(flask.request.endpoint, **args)
app.jinja_env.globals['url_for_args'] = url_for_args

@app.route('/')
def index():
    appdb = get_db()
    filternames = ['type', 'source', 'region', 'channel']
    filters = {}
    for k in filternames:
        if k in flask.request.args:
            filters[k] = flask.request.args[k]
    filtervalues = collections.OrderedDict()
    for k in filternames:
        values = appdb.get_field_values(k, filters)
        if values:
            filtervalues[k] = values

    size = appdb.get_size(filters)
    count = appdb.get_count(filters)

    per_page = 20
    try:
        page = int(flask.request.args['page'])
    except (ValueError, KeyError):
        page = 1
    pagination = goesbrowse.pagination.Pagination(page, per_page, count)

    files = appdb.get_files(filters=filters, limit=per_page, offset=pagination.start - 1)
    
    return flask.render_template('index.html', files=files, size=size, filtervalues=filtervalues, pagination=pagination)

codeFormatter = pygments.formatters.HtmlFormatter()

@app.route('/highlight.css')
def highlight_css():
    data = codeFormatter.get_style_defs('.highlight')
    return flask.Response(data, mimetype='text/css')

@app.route('/<int:id>/<path:slug>/meta')
def meta(id, slug):
    appdb = get_db()
    f = appdb.get_file(id)
    data = json.dumps(json.loads(f['json']), indent=2)
    highlighted = flask.Markup(pygments.highlight(data, pygments.lexers.JsonLexer(), codeFormatter))
    return flask.render_template('meta.html', highlighted=highlighted, file=f)

@app.route('/<int:id>/raw/meta/<path:slug>.json')
def meta_raw(id, slug):
    appdb = get_db()
    f = appdb.get_file(id)
    return flask.Response(f['json'], mimetype='application/json')

@app.route('/<int:id>/<path:slug>/')
def data(id, slug):
    appdb = get_db()
    f = appdb.get_file(id)
    data = None
    if f['type'].lower() == 'txt':
        with open(str(appdb.root / f['datapath'])) as dataf:
            data = dataf.read()
    return flask.render_template('data.html', file=f, data=data)

@app.route('/<int:id>/raw/<path:slug>.<type>')
def data_raw(id, slug, type):
    appdb = get_db()
    f = appdb.get_file(id)
    if app.config['config'].use_x_accel_redirect:
        base = app.config['config'].use_x_accel_redirect
        path = base + f['datapath']
        response = flask.make_response('')
        response.headers['Content-Type'] = ''
        response.headers['X-Accel-Redirect'] = path
        return response
    else:
        return flask.send_file(str(appdb.root / f['datapath']))

