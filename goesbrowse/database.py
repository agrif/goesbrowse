import datetime
import enum
import json
import math
import pathlib
import re

import dateutil.tz
import flask_migrate
import flask_sqlalchemy
import PIL.Image
import sqlalchemy.sql

import goesbrowse.projection

sql = flask_sqlalchemy.SQLAlchemy()
migrate = flask_migrate.Migrate()

class File(sql.Model):
    id = sql.Column(sql.Integer, primary_key=True)
    meta = sql.Column(sql.JSON)
    date = sql.Column(sql.DateTime, index=True)
    type = sql.Column(sql.Text, index=True)
    name = sql.Column(sql.Text, index=True)
    source = sql.Column(sql.Text, index=True)
    region = sql.Column(sql.Text, index=True)
    channel = sql.Column(sql.Text, index=True)

    projection_id = sql.Column(sql.Integer, sql.ForeignKey('projection.id'))
    products = sql.relationship(
        'Product',
        backref=sql.backref('file', lazy=False),
        lazy=False,
    )

    @property
    def localdate(self):
        return self.date.replace(tzinfo=datetime.timezone.utc).astimezone(dateutil.tz.tzlocal())

    def get_product(self, type):
        for prod in self.products:
            if prod.type == type or prod.type.name == type:
                return prod
        return None

class ProductType(enum.IntEnum):
    MAIN = 1
    META = 2
    THUMBNAIL = 3
    TIMELAPSE = 4

    # whether to hide this in the UI
    @property
    def hidden(self):
        return False

class Product(sql.Model):
    id = sql.Column(sql.Integer, primary_key=True)
    path = sql.Column(sql.Text, index=True, unique=True)
    size = sql.Column(sql.Integer, index=True)
    type = sql.Column(sql.Enum(ProductType), index=True)

    file_id = sql.Column(sql.Integer, sql.ForeignKey('file.id'))

    @property
    def ext(self):
        return pathlib.Path(self.path).suffix.lstrip('.').lower()

class Projection(sql.Model):
    id = sql.Column(sql.Integer, primary_key=True)

    # these are sacrosanct: do not modify their names without serious
    # migration munging...
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

        m = re.match('^geos\\(([-+0-9]+\\.?[0-9]*)\\)$', proj_name, re.IGNORECASE)
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
    def __init__(self, root, quota, thumbnail):
        self.quota = quota
        self.root = pathlib.Path(root).resolve()
        self.thumbnail = thumbnail

    def get_size(self, query=None):
        if query is None:
            query = File.query
        query = query.join(Product.file)
        s = query.with_entities(sqlalchemy.sql.func.sum(Product.size)).first()
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
            for prod in file.products:
                excess -= prod.size
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
            for prod in file.products:
                print('deleting', prod.path)
                if not dry_run:
                    try:
                        (self.root / prod.path).unlink()
                    except FileNotFoundError:
                        pass
                    sql.session.delete(prod)
            if not dry_run:
                sql.session.delete(file)
        if not dry_run:
            sql.session.commit()
        self.remove_empty_dirs(self.root, dry_run=dry_run)

    def update_file(self, jsonpath):
        jsonpathrel = jsonpath.relative_to(self.root)
        if Product.query.filter_by(path=str(jsonpathrel)).first():
            # already exists, skip it
            return
        print('updating', jsonpathrel)
        
        with open(str(jsonpath)) as f:
            data = json.load(f)
        
        datapath = (self.root / pathlib.Path(data['Path'])).resolve()
        datapathrel = datapath.relative_to(self.root)
        print('updating', datapathrel)

        datasize = datapath.stat().st_size
        jsonsize = jsonpath.stat().st_size
        suffix = datapathrel.suffix.lstrip('.').lower()

        # attempt some heuristics to split filename
        filedateformat = '%Y%m%dT%H%M%SZ'
        try:
            name, date = jsonpath.stem.rsplit('_', 1)
            date = datetime.datetime.strptime(date, filedateformat)
            meta_from_name = True
            swap_region_channel = False
        except ValueError:
            try:
                # why on earth
                name, date, region = jsonpath.stem.rsplit('_', 2)
                date = datetime.datetime.strptime(date, filedateformat)
                name += '_' + region
                meta_from_name = True
                swap_region_channel = True
            except ValueError:
                date, name = jsonpath.stem.split('_', 1)
                date = datetime.datetime.strptime(date, filedateformat)
                meta_from_name = False
                swap_region_channel = False

        # extract some metadata from the name, if possible
        source = None
        region = None
        channel = None
        if meta_from_name:
            try:
                source, region, channel = name.split('_', 2)
                if swap_region_channel:
                    region, channel = channel, region
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

        proj = None
        im = None
        if suffix == 'jpg':
            im = PIL.Image.open(datapath)
            try:
                proj = Projection.from_nav(im.size[0], im.size[1], data['ImageNavigation'])
                if proj:
                    proj = proj.find_or_insert()
            except KeyError:
                proj = None

        newfile = File(
            meta = data,
            date = date,
            type = suffix,
            name = name,
            source = source,
            region = region,
            channel = channel,
            projection = proj,
        )

        sql.session.add(newfile)

        main = Product(
            path = str(datapathrel),
            size = datasize,
            type = ProductType.MAIN,
            file = newfile,
        )

        meta = Product(
            path = str(jsonpathrel),
            size = jsonsize,
            type = ProductType.META,
            file = newfile,
        )

        sql.session.add(main)
        sql.session.add(meta)

        if self.thumbnail and im is not None:
            thumbpath = datapath.with_suffix('.thumbnail.' + suffix)
            thumbpathrel = thumbpath.relative_to(self.root)
            print('generating', thumbpathrel)
            im.thumbnail((self.thumbnail, self.thumbnail))
            im.save(str(thumbpath))

            thumb = Product(
                path = str(thumbpathrel),
                size = thumbpath.stat().st_size,
                type = ProductType.THUMBNAIL,
                file = newfile,
            )
            sql.session.add(thumb)
