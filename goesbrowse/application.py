import collections
import datetime
import json
import math
import re

import flask
import flask_accept
import flask_caching
import flask_humanize
import geojson
import pygments
import pygments.lexers
import pygments.formatters
import sqlalchemy.sql
import svgwrite
import toml
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
app.jinja_env.globals['ProductType'] = goesbrowse.database.ProductType
app.jinja_env.globals['MapStyle'] = goesbrowse.database.MapStyle
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
            #app.config['SQLALCHEMY_ECHO'] = True
            goesbrowse.database.sql.init_app(app)
            goesbrowse.database.migrate.init_app(app, goesbrowse.database.sql)
        db = flask.g._goesbrowse_database = goesbrowse.database.Database(
            conf.files,
            conf.quota,
            conf.thumbnail,
        )
    return db

GEOJSON_FILES = dict(
    countries='data/geojson/ne_50m_admin_0_countries_lakes.json',
    states='data/geojson/ne_50m_admin_1_states_provinces_lakes.json',
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

def get_data(path, load=toml.load):
    global app
    datacache = getattr(flask.g, '_goesbrowse_data', {})
    if not path in datacache:
        with app.open_resource(path, mode='r') as f:
            datacache[path] = load(f)
        flask.g._goesbrowse_data = datacache
    return datacache[path]

def register_jinja(f):
    global app
    app.jinja_env.globals[f.__name__] = f
    return f

@register_jinja
def get_channels():
    return get_data('data/channels.toml')

@register_jinja
def get_awips_nnn():
    return get_data('data/awips-nnn.toml')

@register_jinja
def human_type(s):
    return s.capitalize()

@register_jinja
def human_source(s):
    if s.lower().startswith('goes'):
        return s.upper()
    if s.lower() == 'nws':
        return s.upper()
    return s.capitalize()

@register_jinja
def human_region(s):
    return s.upper()

@register_jinja
def human_channel(s):
    channels = get_channels()
    s = s.lower()
    if s in channels:
        return channels[s]
    if s[:-1] in channels:
        return channels[s[:-1]]
    return s.upper()

@register_jinja
def human_nnn(s):
    nnns = get_awips_nnn()
    s = s.lower()
    if s in nnns:
        return nnns[s]
    return s.upper()

@register_jinja
def human_style(s):
    s = s.lower()
    if s == 'falsecolor':
        return 'False Color'
    return s.capitalize()

# helper to create a url to current page, with modified args
@register_jinja
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

# helper to create a url with a new filter added
@register_jinja
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
@register_jinja
def url_for_file(file, raw=False):
    if raw:
        return flask.url_for('file_view_raw', p=file.product, type=file.type.name.lower(), ext=file.ext)
    else:
        return flask.url_for('file_view', p=file.product, type=file.type.name.lower())

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

# pagination helper
# (the flask-sqlalchemy one does *weird stuff* to count queries, so...)
class Pagination:
    def __init__(self, query, total, page, per_page):
        self.query = query
        self.total = total
        self.page = page
        self.per_page = per_page

        self.pages = int(math.ceil(total / per_page))
        self.has_next = page >= 1 and page < self.pages
        self.has_prev = page > 1 and page <= self.pages
        self.items = query.limit(per_page).offset((page - 1) * per_page).all()
        self.next_num = page + 1 if self.has_next else None
        self.prev_num = page - 1 if self.has_prev else None

    def page_valid(self, page):
        if page:
            if page <= self.pages and page >= 1:
                return True
        return False

    def page_at(self, page):
        if self.page_valid(page):
            return Pagination(self.query, self.total, page, self.per_page)
        return None

    def next(self):
        return self.page_at(self.next_num)

    def prev(self):
        return self.page_at(self.prev_num)

    def iter_pages(self, left_edge=2, left_current=2, right_current=5, right_edge=2):
        page = 1
        while page <= left_edge:
            if not self.page_valid(page):
                return
            yield page
            page += 1

        if page < self.page - left_current:
            page = self.page - left_current
            yield None

        while page <= self.page + right_current:
            if not self.page_valid(page):
                return
            yield page
            page += 1

        if page < self.pages - right_edge + 1:
            page = self.pages - right_edge + 1
            yield None

        while page <= self.pages:
            if not self.page_valid(page):
                return
            yield page
            page += 1

def query_filters(filters):
    filternames = {
        'type': (goesbrowse.database.Product.type, human_type),
        'source': (goesbrowse.database.Product.source, human_source),
        'region': (goesbrowse.database.MapProduct.region, human_region),
        'channel': (goesbrowse.database.MapProduct.channel, human_channel),
        'subject': (goesbrowse.database.TextProduct.nnn, human_nnn),
        'style': (goesbrowse.database.MapProduct.style, human_style),
    }

    for k in filters:
        if not k in filternames:
            abort(404)

    query = goesbrowse.database.Product.query.with_polymorphic('*')
    count = goesbrowse.database.sql.session.query(sqlalchemy.sql.func.count(goesbrowse.database.Product.id))
    query = query.filter(*[filternames[n][0] == filters[n] for n in filters])
    count = count.filter(*[filternames[n][0] == filters[n] for n in filters])

    filtervalues = collections.OrderedDict()
    filterhumanize = {k: f for k, (_, f) in filternames.items()}
    for k, (c, _) in filternames.items():
        values = query.with_entities(c).distinct()
        if values:
            filtervalues[k] = [v[0] for v in values if v[0]]
            filtervalues[k].sort()
            filtervalues[k] = [(v.name if hasattr(v, 'name') else v) for v in filtervalues[k]]

    #size = query.join(goesbrowse.database.File).with_entities(sqlalchemy.sql.func.sum(goesbrowse.database.File.size)).first()
    #if size is None:
    #    size = 0
    #else:
    #    size = size[0]

    return (query, count, filtervalues, filterhumanize)

@app.route('/', defaults={'filters': {}})
@app.route('/<filter:filters>')
def index(filters):
    query, count, filtervalues, filterhumanize = query_filters(filters)

    per_page = 20
    try:
        page = int(flask.request.args['page'])
    except (ValueError, KeyError):
        page = 1

    query = query.order_by(goesbrowse.database.Product.date.desc())
    pagination = Pagination(query, count.first_or_404()[0], page, per_page)

    #import flask_sqlalchemy
    #import pprint
    #pprint.pprint(flask_sqlalchemy.get_debug_queries())

    return flask.render_template('index.html', products=pagination.items, filtervalues=filtervalues, filters=filters, filterhumanize=filterhumanize, pagination=pagination)

@app.route('/latest', defaults={'filters': {}, 'type': 'main'})
@app.route('/<filter:filters>/latest/', defaults={'type': 'main'})
@app.route('/<filter:filters>/latest/<type>')
def latest_view(filters, type):
    query, _, _, _ = query_filters(filters)
    query = query.order_by(goesbrowse.database.Product.date.desc())
    product = query.first_or_404()
    return flask.redirect(flask.url_for('file_view', p=product, type=type))

@app.route('/highlight.css')
@cache.cached(timeout=VERY_LONG_TIME)
def highlight_css():
    data = codeFormatter.get_style_defs('.highlight')
    response = flask.Response(data, mimetype='text/css')
    response.cache_control.max_age = VERY_LONG_TIME
    return response

@app.route('/<product:p>/', defaults={'type': 'main'})
@app.route('/<product:p>/<type>/')
@flask_accept.accept_fallback
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

@file_view.support('application/json')
def file_view_json(p, type):
    p = goesbrowse.database.Product.query.get(p)
    # we ignore type for json requests -- we just dump everything!

    # a bit of a hack to jsonify models without too much work
    result = {}
    for c in p.__table__.columns:
        if not hasattr(p, c.name):
            continue

        key = c.name
        val = getattr(p, c.name)
        # touchups...
        if c.name == 'id':
            continue
        elif c.name == 'date':
            val = val.isoformat()
        elif c.name == 'projection_id':
            key = 'map'
            val = flask.url_for('map', id=p.projection.id)
        elif hasattr(val, 'name'):
            val = val.name

        result[key] = val

    # and just provide the related file urls...
    result['files'] = {}
    for f in p.files:
        result['files'][f.type.name] = url_for_file(f, raw=True)

    return flask.jsonify(**result)

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
