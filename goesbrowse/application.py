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
import werkzeug.routing

import goesbrowse.config
import goesbrowse.database
import goesbrowse.projection

app = flask.Flask(__name__)
app.config['CACHE_TYPE'] = 'simple'
humanize = flask_humanize.Humanize(app)
cache = flask_caching.Cache(app)
VERY_LONG_TIME = 60 * 60 * 24
app.jinja_env.globals['FileType'] = goesbrowse.database.FileType
app.jinja_env.add_extension('jinja2_highlight.HighlightExtension')
app.jinja_env.extend(jinja2_highlight_cssclass = 'highlight')

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
            conf.thumbnail,
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

# helper to create a url with a new filter added
def url_for_filters(**kwargs):
    args = flask.request.view_args.copy()
    args['filters'] = args.get('filters', {}).copy()
    for k, v in kwargs.items():
        if v is None:
            if k in args['filters']:
                del args['filters'][k]
        else:
            args['filters'][k] = v
    return flask.url_for(flask.request.endpoint, **args)
app.jinja_env.globals['url_for_filters'] = url_for_filters

# turn a url part into a product, directly
class ProductConverter(werkzeug.routing.BaseConverter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.regex = "\d+/" + "/".join(["[^/]+"] * 3)

    def to_python(self, value):
        i = int(value.split('/', 2)[0])
        # FIXME use newer flask to be able to fetch object here
        return i

    def to_url(self, value):
        date = value.date.strftime('%Y-%m-%d')
        time = value.date.strftime('%H.%M.%S')
        return "{}/{}/{}/{}".format(value.id, date, time, value.name)
app.url_map.converters['product'] = ProductConverter

# helper to make a url to a product, raw or not
def url_for_file(file, raw=False):
    if raw:
        return flask.url_for('file_view_raw', p=file.product, type=file.type.name.lower(), ext=file.ext)
    else:
        return flask.url_for('file_view', p=file.product, type=file.type.name.lower())
app.jinja_env.globals['url_for_file'] = url_for_file

# turn a url part into substitute query parameters
class FilterConverter(werkzeug.routing.BaseConverter):
    regex = '[^/:]+:[^/]+(?:/[^/:]+:[^/]+)*'

    def to_python(self, value):
        parts = (part.split(':', 2) for part in value.split('/'))
        return dict(parts)

    def to_url(self, value):
        url = '/'.join(str(k) + ':' + str(v) for k, v in sorted(value.items()))
        return werkzeug.routing._fast_url_quote(url.encode(self.map.charset))
app.url_map.converters['filter'] = FilterConverter

@app.route('/', defaults={'filters': {}})
@app.route('/<filter:filters>')
def index(filters):
    filternames = ['type', 'source', 'region', 'channel']
    for k in filters:
        if not k in filternames:
            abort(404)

    query = goesbrowse.database.Product.query
    query = query.filter_by(**filters)

    filtervalues = collections.OrderedDict()
    for k in filternames:
        values = query.with_entities(getattr(goesbrowse.database.Product, k)).distinct()
        if values:
            filtervalues[k] = [v[0] for v in values if v[0]]
            filtervalues[k].sort()

    size = query.join(goesbrowse.database.File).with_entities(sqlalchemy.sql.func.sum(goesbrowse.database.File.size)).first()
    if size is None:
        size = 0
    else:
        size = size[0]

    per_page = 20
    try:
        page = int(flask.request.args['page'])
    except (ValueError, KeyError):
        page = 1

    query = query.order_by(goesbrowse.database.Product.date.desc())
    pagination = query.paginate(page, per_page)

    return flask.render_template('index.html', products=pagination.items, size=size, filtervalues=filtervalues, filters=filters, pagination=pagination)

@app.route('/highlight.css')
@cache.cached(timeout=VERY_LONG_TIME)
def highlight_css():
    data = codeFormatter.get_style_defs('.highlight')
    response = flask.Response(data, mimetype='text/css')
    response.cache_control.max_age = VERY_LONG_TIME
    return response

@app.route('/<product:p>/', defaults={'type': 'main'})
@app.route('/<product:p>/<type>/')
def file_view(p, type):
    p = goesbrowse.database.Product.query.get(p)
    file = p.get_file(type.upper())
    if not file:
        flask.abort(404)

    content = None
    if file.ext == 'txt':
        appdb = get_db()
        with open(str(appdb.root / file.path), 'r') as dataf:
            content = dataf.read()
    if file.ext == 'json':
        content = json.dumps(p.meta, indent=2)
    return flask.render_template('file.html', product=p, file=file, content=content)

@app.route('/<product:p>.<ext>', defaults={'type': 'main'})
@app.route('/<product:p>.<type>.<ext>')
def file_view_raw(p, type, ext):
    p = goesbrowse.database.Product.query.get(p)
    file = p.get_file(type.upper())
    if not file:
        flask.abort(404)

    appdb = get_db()
    conf = get_config()
    if conf.use_x_accel_redirect:
        base = conf.use_x_accel_redirect
        path = base + file.path
        response = flask.make_response('')
        response.headers['Content-Type'] = ''
        response.headers['X-Accel-Redirect'] = path
        return response
    else:
        return flask.send_file(str(appdb.root / file.path))

@app.route('/map/<int:id>.svg')
@cache.cached(timeout=VERY_LONG_TIME)
def map(id):
    proj = goesbrowse.database.Projection.query.get_or_404(id)

    geo = get_geojson()
    d = svgwrite.Drawing(size=(proj.width, proj.height))
    d.viewbox(0, 0, proj.width, proj.height)

    def simplify(pts):
        #yield from (p for p in pts if p)
        #return
        lastx = None
        lasty = None
        for pt in pts:
            if not pt:
                continue
            x = round(pt[0], 1)
            y = round(pt[1], 1)
            if lastx is None:
                lastx = x
                lasty = y
                yield (x, y)
            else:
                if (x - lastx) ** 2 + (y - lasty) ** 2 >= 1.0:
                    lastx = x
                    lasty = y
                    yield(x, y)

    def draw_polygon(lines, poly):
        pts = (proj.forward(*p) for p in poly)
        if not any(p[0] >= 0 and p[0] < proj.width and p[1] >= 0 and p[1] < proj.height for p in pts if p):
            return
        if pts:
            pts = list(simplify(pts))
            if pts:
                lines.add(d.polygon(simplify(pts)))

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
