import datetime
import json
import math
import pathlib
import re

import dateutil.tz
import flask_migrate
import flask_sqlalchemy
import sqlalchemy.sql

import goesbrowse.projection

sql = flask_sqlalchemy.SQLAlchemy()
migrate = flask_migrate.Migrate()

class File(sql.Model):
    id = sql.Column(sql.Integer, primary_key=True)
    jsonpath = sql.Column(sql.Text, index=True, unique=True)
    datapath = sql.Column(sql.Text, index=True, unique=True)
    json = sql.Column(sql.JSON)
    date = sql.Column(sql.DateTime, index=True)
    size = sql.Column(sql.Integer, index=True)
    type = sql.Column(sql.Text, index=True)
    name = sql.Column(sql.Text, index=True)
    source = sql.Column(sql.Text, index=True)
    region = sql.Column(sql.Text, index=True)
    channel = sql.Column(sql.Text, index=True)

    projection_id = sql.Column(sql.Integer, sql.ForeignKey('projection.id'))

    @property
    def slug(self):
        return self.date.strftime('%Y-%m-%d/%H.%M.%S/') + self.name

    @property
    def localdate(self):
        return self.date.replace(tzinfo=datetime.timezone.utc).astimezone(dateutil.tz.tzlocal())

class Projection(sql.Model):
    id = sql.Column(sql.Integer, primary_key=True)
    width = sql.Column(sql.Integer)
    height = sql.Column(sql.Integer)
    x_offset = sql.Column(sql.Integer)
    y_offset = sql.Column(sql.Integer)
    x_scale = sql.Column(sql.Integer)
    y_scale = sql.Column(sql.Integer)
    lon_0 = sql.Column(sql.Float)

    files = sql.relationship(
        'File',
        backref=sql.backref('projection', lazy=True),
        lazy=True,
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.reload()

    @sql.reconstructor
    def reload(self):
        self.proj = goesbrowse.projection.GeosProj(h=35786023.0, sweep='x', lon_0=self.lon_0)

    def forward(self, lam, phi):
        lam = math.radians(lam)
        phi = math.radians(phi)
        pt = self.proj.forward(lam, phi)
        if pt is None:
            return None
        x, y = pt
        x *= self.x_scale * goesbrowse.projection.SCALE_FACTOR
        x += self.x_offset
        y *= -self.y_scale * goesbrowse.projection.SCALE_FACTOR
        y += self.y_offset
        return x, y

    def find_or_insert(self):
        found = self.query.filter_by(
            width=self.width,
            height=self.height,
            x_offset=self.x_offset,
            y_offset=self.y_offset,
            x_scale=self.x_scale,
            y_scale=self.y_scale,
            lon_0=self.lon_0,
        ).first()
        if found:
            return found

        sql.session.add(self)
        return self

    @classmethod
    def from_nav(cls, width, height, nav):
        try:
            x_offset = nav['ColumnOffset']
            y_offset = nav['LineOffset']
            x_scale = nav['ColumnScaling']
            y_scale = nav['LineScaling']
            proj_name = nav['ProjectionName']
        except KeyError:
            return None

        m = re.match('^geos\\(([-+0-9]+\\.?[0-9]*)\\)$', proj_name)
        if not m:
            return None
        lon_0 = float(m.group(1))

        return cls(
            width=width,
            height=height,
            x_offset=x_offset,
            y_offset=y_offset,
            x_scale=x_scale,
            y_scale=y_scale,
            lon_0=lon_0,
        )

    @classmethod
    def from_meta(cls, meta):
        try:
            width = meta['SegmentIdentification']['MaxColumn']
            height = meta['SegmentIdentification']['MaxLine']
        except KeyError:
            try:
                width = meta['ImageStructure']['Columns']
                height = meta['ImageStructure']['Lines']
            except KeyError:
                return None
        try:
            nav = meta['ImageNavigation']
        except KeyError:
            return None

        return cls.from_nav(width, height, nav)

class Database:
    def __init__(self, root, quota):
        self.quota = quota
        self.root = pathlib.Path(root).resolve()

    def get_size(self, query=None):
        if query is None:
            query = File.query
        s = query.with_entities(sqlalchemy.sql.func.sum(File.size)).first()
        if not s:
            return 0
        return s[0]

    @property
    def size(self):
        return self.get_size()

    def get_above_quota(self):
        if not self.quota:
            return

        excess = self.size - self.quota
        if excess <= 0:
            return

        # fixme: https://gist.github.com/Gizmokid2005/2bb9cc3746f4f0ea0dbfb83e7d64a8da
        for file in File.query.order_by(File.date).all():
            excess -= file.size
            yield file
            if excess <= 0:
                break

    def remove_empty_dirs(self, path, dry_run=False):
        path = path.resolve()
        for sub in path.iterdir():
            if not sub.is_dir():
                continue
            self.remove_empty_dirs(sub)
        if path != self.root and len(list(path.iterdir())) == 0:
            print('removing', path.relative_to(self.root))
            if not dry_run:
                path.rmdir()

    def update(self):
        for jsonpath in self.root.rglob('*.json'):
            self.update_file(jsonpath)
        sql.session.commit()

    def clean(self, dry_run=False):
        for file in list(self.get_above_quota()):
            print('deleting', file.datapath)
            if not dry_run:
                sql.session.delete(file)
                (self.root / file.datapath).unlink()
                (self.root / file.jsonpath).unlink()
        if not dry_run:
            sql.session.commit()
        self.remove_empty_dirs(self.root, dry_run=dry_run)

    def update_file(self, jsonpath):
        jsonpathrel = jsonpath.relative_to(self.root)
        if File.query.filter_by(jsonpath=str(jsonpathrel)).first():
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

        proj = Projection.from_meta(data)
        if proj:
            proj = proj.find_or_insert()

        newfile = File(
            jsonpath = str(jsonpathrel),
            datapath = str(datapathrel),
            json = data,
            date = date,
            size = size,
            type = suffix,
            name = name,
            source = source,
            region = region,
            channel = channel,
            projection = proj,
        )

        sql.session.add(newfile)

