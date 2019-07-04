import os
import pathlib
import json
import datetime
import math
import collections

import goesbrowse.config

import pygments
import pygments.lexers
import pygments.formatters
import dateutil.tz
import click
import sqlite3
import flask

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
        db = flask.g.database = Database(conf.files, conf.database, conf.quota)
    return db

class Database:
    def __init__(self, root, db, quota):
        self.quota = quota
        self.root = pathlib.Path(root).resolve()
        self.dbpath = pathlib.Path(db).resolve()

        self.db = sqlite3.connect(str(self.dbpath))
        self.db.row_factory = sqlite3.Row

        self.setup()

    def setup(self):
        self.query('create table if not exists goesmeta (name text, value text)')
        try:
            version = self.version
            if version != 0:
                raise RuntimeError('unknown version, please remake database')
            # the db is ready, and the correct version, so...
            return
        except KeyError:
            # need to create database...
            pass

        self.query('create unique index idx_goesmeta_name on goesmeta (name)')
        self.set_metadata('version', 0)

        self.query('''create table goesfiles (
        jsonpath text,
        datapath text,
        json text,
        date integer,
        size integer,
        type text,
        name text,
        source text,
        region text,
        channel text
        )''')

        self.query('create unique index idx_goesfiles_jsonpath on goesfiles (jsonpath)')
        self.query('create unique index idx_goesfiles_datapath on goesfiles (datapath)')
        self.query('create index idx_goesfiles_date on goesfiles (date)')
        self.query('create index idx_goesfiles_type on goesfiles (type)')
        self.query('create index idx_goesfiles_name on goesfiles (name)')
        self.query('create index idx_goesfiles_source on goesfiles (source)')
        self.query('create index idx_goesfiles_region on goesfiles (region)')
        self.query('create index idx_goesfiles_channel on goesfiles (channel)')

        self.commit()

    def query(self, query, *args):
        cur = self.db.execute(query, args)
        rv = cur.fetchall()
        cur.close()
        return rv

    def query_one(self, query, *args):
        rv = self.query(query, *args)
        if rv:
            return rv[0]
        return None

    def commit(self):
        self.db.commit()

    def get_metadata(self, name):
        v = self.query_one('select value from goesmeta where name = ?', name)
        if v is None:
            raise KeyError('bad metadata key')
        return v['value']

    def set_metadata(self, name, value):
        self.query('replace into goesmeta (name, value) values (?, ?)', name, str(value))

    @property
    def version(self):
        return int(self.get_metadata('version'))

    def convert_file(self, f):
        f = dict(f)
        f['name'] = f['name'].replace('_', '-')
        f['date'] = datetime.datetime.fromtimestamp(f['date']).replace(tzinfo=dateutil.tz.tzlocal())
        f['slug'] = f['date'].strftime('%Y-%m-%d/%H.%M.%S/') + f['name']
        return f

    def make_where_clause(self, filters):
        wheres = []
        whereargs = []
        for k, v in filters.items():
            wheres.append('{} = ?'.format(k))
            whereargs.append(v)
        where = ''
        if wheres:
            where = 'where ' + ' and '.join(wheres)
        return where, whereargs

    def get_field_values(self, field, filters={}):
        where, whereargs = self.make_where_clause(filters)
        files = self.query('select distinct {} from goesfiles {}'.format(field, where), *whereargs)
        values = [f[field] for f in files if f[field]]
        values.sort()
        return values

    def get_files(self, filters={}, limit=None, offset=None):
        where, whereargs = self.make_where_clause(filters)
        query = 'select rowid, * from goesfiles'
        if where:
            query += ' ' + where
        query += ' order by date desc'
        if limit is not None:
            query += ' limit {}'.format(limit)
        if offset is not None:
            query += ' offset {}'.format(offset)
        
        files = self.query(query, *whereargs)
        return [self.convert_file(f) for f in files]

    def get_file(self, id):
        f = self.query_one('select rowid, * from goesfiles where rowid = ?', id)
        if f:
            return self.convert_file(f)
        return None

    def get_size(self, filters={}):
        where, whereargs = self.make_where_clause(filters)
        return self.query_one('select sum(size) from goesfiles {}'.format(where), *whereargs)[0]

    @property
    def size(self):
        return self.get_size()

    def get_count(self, filters={}):
        where, whereargs = self.make_where_clause(filters)
        return self.query_one('select count(rowid) from goesfiles {}'.format(where), *whereargs)[0]

    @property
    def count(self):
        return self.get_count()

    def get_above_quota(self):
        excess = self.size - self.quota
        if excess <= 0:
            return
        # fixme: https://gist.github.com/Gizmokid2005/2bb9cc3746f4f0ea0dbfb83e7d64a8da
        for file in self.query('select rowid, size, datapath, jsonpath from goesfiles order by date'):
            excess -= file['size']
            yield file
            if excess <= 0:
                break

    def remove_empty_dirs(self, path):
        path = path.resolve()
        for sub in path.iterdir():
            if not sub.is_dir():
                continue
            self.remove_empty_dirs(sub)
        if path != self.root and len(list(path.iterdir())) == 0:
            print('removing', path.relative_to(self.root))
            path.rmdir()

    def update(self):
        for jsonpath in self.root.rglob('*.json'):
            self.update_file(jsonpath)
        for file in list(self.get_above_quota()):
            print('deleting', file['datapath'])
            self.query('delete from goesfiles where rowid = ?', file['rowid'])
            (self.root / file['datapath']).unlink()
            (self.root / file['jsonpath']).unlink()
        self.commit()
        self.remove_empty_dirs(self.root)

    def update_file(self, jsonpath):
        jsonpathrel = jsonpath.relative_to(self.root)
        if self.query_one('select rowid from goesfiles where jsonpath = ?', str(jsonpathrel)):
            # already exists, skip it
            return
        print('updating', jsonpathrel)
        
        with open(str(jsonpath)) as f:
            data = json.load(f)
        
        datapath = (self.root / pathlib.Path(data['Path'])).resolve()
        datapathrel = datapath.relative_to(self.root)
        size = datapath.stat().st_size
        suffix = datapathrel.suffix.lstrip('.')

        # attempt some heuristics to split filename
        filedateformat = '%Y%m%dT%H%M%SZ'
        try:
            name, date = jsonpath.stem.rsplit('_', 1)
            date = datetime.datetime.strptime(date, filedateformat)
            namefirst = True
        except ValueError:
            date, name = jsonpath.stem.split('_', 1)
            date = datetime.datetime.strptime(date, filedateformat)
            namefirst = False

        # extract some metadata from the name, if possible
        source = None
        region = None
        channel = None
        if namefirst:
            try:
                source, region, channel = name.split('_', 2)
            except ValueError:
                pass
        else:
            source = jsonpathrel.parts[0]

        # use the json date, if it exists
        try:
            date = data['TimeStamp']['ISO8601']
            date = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ')
        except KeyError:
            pass

        # give our date a timezone
        date = date.replace(tzinfo=datetime.timezone.utc)

        self.query('''replace into goesfiles (
        jsonpath, datapath, json, date, size, type,
        name, source, region, channel
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                   str(jsonpathrel),
                   str(datapathrel),
                   json.dumps(data),
                   date.timestamp(),
                   size,
                   suffix,
                   name,
                   source,
                   region,
                   channel,
        )

# http://flask.pocoo.org/snippets/44/
class Pagination(object):
    def __init__(self, page, per_page, total_count):
        self.page = page
        self.per_page = per_page
        self.total_count = total_count

    @property
    def start(self):
        return (self.page - 1) * self.per_page + 1

    @property
    def end(self):
        return self.page * self.per_page

    @property
    def pages(self):
        return int(math.ceil(self.total_count / float(self.per_page)))

    @property
    def has_prev(self):
        return self.page > 1

    @property
    def has_next(self):
        return self.page < self.pages

    def iter_pages(self, left_edge=2, left_current=2, right_current=5, right_edge=2):
        last = 0
        for num in range(1, self.pages + 1):
            if num <= left_edge or \
               (num > self.page - left_current - 1 and \
                num < self.page + right_current) or \
                num > self.pages - right_edge:
                if last + 1 != num:
                    yield None
                yield num
                last = num

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
    pagination = Pagination(page, per_page, count)

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

@click.group(cls=flask.cli.FlaskGroup, create_app=lambda scriptinfo: app)
@click.option('--config')
def cli(config):
    app.config['configpath'] = config

@cli.command()
def updatedb():
    appdb = get_db()
    appdb.update()

@cli.command()
def timelapse():
    appdb = get_db()
    # ffmpeg -f concat -i fstest.txt -vf "fps=10, scale=512:-1, drawtext=text='%{metadata\:imagedate}': fontcolor=0xaaaaaa: font=mono: fontsize=14: x=10: y=h-th-10" -pix_fmt yuv420p -c:v libx264 -preset veryslow -crf 19 -profile:v high -level 4.2 -movflags +faststart output.mp4
    postroll = 5
    rate = 1 / (8 * 60 * 60)

    files = appdb.get_files(filters=dict(region='FD', source='GOES16', channel='CH13_enhanced'))
    files.reverse()
    lastdate = None
    print('ffconcat version 1.0')
    for f in files:
        if lastdate is not None:
            duration = f['date'] - lastdate
            print('duration {}'.format(duration.total_seconds() * rate))
        print('file {}'.format(f['datapath']))
        print('file_packet_metadata imagedate=\'{:%a %b %d %Y, %H:%M:%S %Z}\''.format(f['date']))
        lastdate = f['date']
    print('duration {}'.format(postroll))

if __name__ == '__main__':
    cli()
