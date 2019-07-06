import collections
import json
import math
import re

import flask
import flask_caching
import flask_humanize
import geojson
import pygments
import pygments.lexers
import pygments.formatters
import sqlalchemy.sql
import svgwrite

import goesbrowse.config
import goesbrowse.database
import goesbrowse.projection

app = flask.Flask(__name__)
app.config['CACHE_TYPE'] = 'simple'
humanize = flask_humanize.Humanize(app)
cache = flask_caching.Cache(app)
VERY_LONG_TIME = 60 * 60 * 24

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

GEOJSON_FILES = dict(
    countries='geojson/ne_50m_admin_0_countries_lakes.json',
    states='geojson/ne_50m_admin_1_states_provinces_lakes.json',
)

def get_geojson():
    global app
    geo = getattr(flask.g, '_goesbrowse_geojson', None)
    if geo is None:
        geo = {}
        for k, v in GEOJSON_FILES.items():
            with app.open_resource(v, mode='r') as f:
                geo[k] = geojson.load(f)
        flask.g._goesbrowse_geojson = geo
    return geo

# helper to create a url to current page, with modified args
def url_for_args(**kwargs):
    args = flask.request.view_args.copy()
    for k, v in flask.request.args.items():
        args[k] = v
    for k, v in kwargs.items():
        if v is None or (k == 'page' and v == 1):
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

    size = query.join(goesbrowse.database.Product).with_entities(sqlalchemy.sql.func.sum(goesbrowse.database.Product.size)).first()
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
@cache.cached(timeout=VERY_LONG_TIME)
def highlight_css():
    data = codeFormatter.get_style_defs('.highlight')
    response = flask.Response(data, mimetype='text/css')
    response.cache_control.max_age = VERY_LONG_TIME
    return response

@app.route('/<int:id>/<path:slug>/meta')
def meta(id, slug):
    f = goesbrowse.database.File.query.get_or_404(id)
    data = json.dumps(f.meta, indent=2)
    highlighted = flask.Markup(pygments.highlight(data, pygments.lexers.JsonLexer(), codeFormatter))
    return flask.render_template('meta.html', highlighted=highlighted, file=f)

@app.route('/<int:id>/raw/meta/<path:slug>.json')
def meta_raw(id, slug):
    f = goesbrowse.database.File.query.get_or_404(id)
    return flask.Response(json.dumps(f.meta), mimetype='application/json')

@app.route('/<int:id>/<path:slug>/')
def data(id, slug):
    appdb = get_db()
    f = goesbrowse.database.File.query.get_or_404(id)
    data = None
    if f.type.lower() == 'txt':
        with open(str(appdb.root / f.get_product('MAIN').path)) as dataf:
            data = dataf.read()
    return flask.render_template('data.html', file=f, product=f.get_product('MAIN'), data=data)

@app.route('/<int:id>/raw/<path:slug>.<type>')
def data_raw(id, slug, type):
    f = goesbrowse.database.File.query.get_or_404(id)
    appdb = get_db()
    conf = get_config()
    if conf.use_x_accel_redirect:
        base = conf.use_x_accel_redirect
        path = base + f.get_product('MAIN').path
        response = flask.make_response('')
        response.headers['Content-Type'] = ''
        response.headers['X-Accel-Redirect'] = path
        return response
    else:
        return flask.send_file(str(appdb.root / f.get_product('MAIN').path))

@app.route('/map/<int:id>.svg')
@cache.cached(timeout=VERY_LONG_TIME)
def map_raw(id):
    proj = goesbrowse.database.Projection.query.get_or_404(id)

    geo = get_geojson()
    d = svgwrite.Drawing(size=(proj.width, proj.height))
    d.viewbox(0, 0, proj.width, proj.height)

    def draw_polygon(lines, poly):
        pts = (proj.forward(*pt) for pt in poly)
        pts = [pt for pt in pts if pt]
        if not any([p[0] >= 0 and p[0] < proj.width and p[1] >= 0 and p[1] < proj.height for p in pts]):
            return
        if pts:
            lines.add(d.polygon(pts))

    def draw_geometry(lines, geom):
        if isinstance(geom, geojson.Polygon):
            for poly in geom.coordinates:
                draw_polygon(lines, poly)
        elif isinstance(geom, geojson.MultiPolygon):
            for multi in geom.coordinates:
                for poly in multi:
                    draw_polygon(lines, poly)

    for k, v in geo.items():
        lines = d.add(d.g(fill='none', stroke='white', stroke_width=5, stroke_opacity=0.5, id=k))
        geojson.utils.map_geometries(lambda g: draw_geometry(lines, g), v)

    response = flask.Response(d.tostring(), mimetype='image/svg+xml')
    response.cache_control.max_age = VERY_LONG_TIME
    return response
