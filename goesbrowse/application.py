import collections
import json

import flask
import pygments
import pygments.lexers
import pygments.formatters
import sqlalchemy.sql

import goesbrowse.config
import goesbrowse.database

app = flask.Flask(__name__)

codeFormatter = pygments.formatters.HtmlFormatter()

def get_config():
    conf = getattr(flask.g, '_goesbrowse_config', None)
    if conf is None:
        extras = [app.config.get('GOESBROWSE_CONFIG_PATH')]
        conf = flask.g._goesbrowse_config = goesbrowse.config.discover(extras)
    return conf

@app.before_first_request
def get_db():
    global app
    db = getattr(flask.g, '_goesbrowse_database', None)
    if db is None:
        conf = get_config()
        if not app.config.get('SQLALCHEMY_DATABASE_URI'):
            app.config['SQLALCHEMY_DATABASE_URI'] = conf.database
            app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
            goesbrowse.database.sql.init_app(app)
            goesbrowse.database.migrate.init_app(app, goesbrowse.database.sql)
        db = flask.g._goesbrowse_database = goesbrowse.database.Database(
            conf.files,
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
    filternames = ['type', 'source', 'region', 'channel']
    filters = {}
    for k in filternames:
        if k in flask.request.args:
            filters[k] = flask.request.args[k]

    query = goesbrowse.database.File.query
    query = query.filter_by(**filters)

    filtervalues = collections.OrderedDict()
    for k in filternames:
        values = query.with_entities(getattr(goesbrowse.database.File, k)).distinct()
        if values:
            filtervalues[k] = [v[0] for v in values if v[0]]
            filtervalues[k].sort()

    size = query.with_entities(sqlalchemy.sql.func.sum(goesbrowse.database.File.size)).first()
    if size is None:
        size = 0
    else:
        size = size[0]

    per_page = 20
    try:
        page = int(flask.request.args['page'])
    except (ValueError, KeyError):
        page = 1

    query = query.order_by(goesbrowse.database.File.date.desc())
    pagination = query.paginate(page, per_page)

    return flask.render_template('index.html', files=pagination.items, size=size, filtervalues=filtervalues, pagination=pagination)

@app.route('/highlight.css')
def highlight_css():
    data = codeFormatter.get_style_defs('.highlight')
    return flask.Response(data, mimetype='text/css')

@app.route('/<int:id>/<path:slug>/meta')
def meta(id, slug):
    f = goesbrowse.database.File.query.get_or_404(id)
    data = json.dumps(f.json, indent=2)
    highlighted = flask.Markup(pygments.highlight(data, pygments.lexers.JsonLexer(), codeFormatter))
    return flask.render_template('meta.html', highlighted=highlighted, file=f)

@app.route('/<int:id>/raw/meta/<path:slug>.json')
def meta_raw(id, slug):
    f = goesbrowse.database.File.query.get_or_404(id)
    return flask.Response(json.dumps(f.json), mimetype='application/json')

@app.route('/<int:id>/<path:slug>/')
def data(id, slug):
    appdb = get_db()
    f = goesbrowse.database.File.query.get_or_404(id)
    data = None
    if f.type.lower() == 'txt':
        with open(str(appdb.root / f.datapath)) as dataf:
            data = dataf.read()
    return flask.render_template('data.html', file=f, data=data)

@app.route('/<int:id>/raw/<path:slug>.<type>')
def data_raw(id, slug, type):
    f = goesbrowse.database.File.query.get_or_404(id)
    appdb = get_db()
    conf = get_config()
    if conf.use_x_accel_redirect:
        base = conf.use_x_accel_redirect
        path = base + f.datapath
        response = flask.make_response('')
        response.headers['Content-Type'] = ''
        response.headers['X-Accel-Redirect'] = path
        return response
    else:
        return flask.send_file(str(appdb.root / f.datapath))

