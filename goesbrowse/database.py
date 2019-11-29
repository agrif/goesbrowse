import datetime
import enum
import json
import math
import pathlib
import re
import traceback

import dateutil.tz
import flask_migrate
import flask_sqlalchemy
import PIL.Image
import sqlalchemy.sql

import goesbrowse.projection
import goesbrowse.application # only used for get_awips_nnn

sql = flask_sqlalchemy.SQLAlchemy()
migrate = flask_migrate.Migrate()

class ProductType(enum.IntEnum):
    TEXT = 1
    IMAGE = 2
    MAP = 3

class Product(sql.Model):
    id = sql.Column(sql.Integer, primary_key=True)
    type = sql.Column(sql.Enum(ProductType), index=True)
    meta = sql.Column(sql.JSON)

    source = sql.Column(sql.Text, index=True)
    date = sql.Column(sql.DateTime, index=True)
    name = sql.Column(sql.Text, index=True)

    files = sql.relationship(
        'File',
        backref=sql.backref('product', lazy=False),
        lazy=False,
    )

    __mapper_args__ = {
        'polymorphic_on': type,
    }

    @property
    def localdate(self):
        return self.date.replace(tzinfo=datetime.timezone.utc).astimezone(dateutil.tz.tzlocal())

    @property
    def ext(self):
        return self.get_file(FileType.MAIN).ext

    def get_file(self, type):
        for f in self.files:
            if f.type == type or f.type.name == type:
                return f
        return None

class TextProduct(Product):
    # awips, may be none
    nnn = sql.Column(sql.Text, index=True)
    xxx = sql.Column(sql.Text, index=True)

    __mapper_args__ = {
        'polymorphic_identity': ProductType.TEXT,
    }

class ImageProduct(Product):
    width = sql.Column(sql.Integer, index=True)
    height = sql.Column(sql.Integer, index=True)

    __mapper_args__ = {
        'polymorphic_identity': ProductType.IMAGE,
    }

class MapStyle(enum.IntEnum):
    NORMAL = 1
    ENHANCED = 2
    FALSECOLOR = 3

class MapProduct(ImageProduct):
    region = sql.Column(sql.Text, index=True)
    channel = sql.Column(sql.Text, index=True)
    style = sql.Column(sql.Enum(MapStyle), index=True)
    projection_id = sql.Column(sql.Integer, sql.ForeignKey('projection.id'))

    __mapper_args__ = {
        'polymorphic_identity': ProductType.MAP,
    }

# all product types should be defined by here, because we are about to
# create an OMEGA-INDEX to help joint filters
sql.Index('idx_filter', Product.type, Product.source, MapProduct.region, MapProduct.channel, MapProduct.style, TextProduct.nnn)

class FileType(enum.IntEnum):
    MAIN = 1
    META = 2
    THUMBNAIL = 3
    TIMELAPSE = 4

    # whether to hide this in the UI
    @property
    def hidden(self):
        if self == self.THUMBNAIL:
            return True
        return False

class File(sql.Model):
    id = sql.Column(sql.Integer, primary_key=True)
    path = sql.Column(sql.Text, index=True, unique=True)
    size = sql.Column(sql.Integer, index=True)
    type = sql.Column(sql.Enum(FileType), index=True)

    product_id = sql.Column(sql.Integer, sql.ForeignKey('product.id'))

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

    products = sql.relationship(
        'MapProduct',
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
        query = query.join(File.product)
        s = query.with_entities(sqlalchemy.sql.func.sum(File.size)).first()
        if not s:
            return 0
        return s[0]

    @property
    def size(self):
        return self.get_size()

    def get_above_quota(self, page_size=10):
        if not self.quota:
            return

        excess = self.size - self.quota
        if excess <= 0:
            return

        # fixme: https://gist.github.com/Gizmokid2005/2bb9cc3746f4f0ea0dbfb83e7d64a8da
        offset = 0
        while excess > 0:
            for prod in Product.query.order_by(Product.date).limit(page_size).offset(offset).all():
                for file in prod.files:
                    excess -= file.size
                yield prod
                offset += 1
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
            try:
                self.update_file(jsonpath)
            except Exception as e:
                # we must continue, or quotas won't even barely work...
                # shouldn't touch the database unless everything else works first
                # but print a traceback, at least
                traceback.print_exc()
        print('committing...')
        sql.session.commit()
        print('done.')

    def clean(self, dry_run=False):
        for prod in list(self.get_above_quota()):
            for file in prod.files:
                print('deleting', file.path)
                if not dry_run:
                    try:
                        (self.root / file.path).unlink()
                    except FileNotFoundError:
                        pass
                    sql.session.delete(file)
            if not dry_run:
                sql.session.delete(prod)
        if not dry_run:
            print('committing...')
            sql.session.commit()
            print('done.')
        self.remove_empty_dirs(self.root, dry_run=dry_run)

    def update_file(self, jsonpath):
        jsonpathrel = jsonpath.relative_to(self.root)
        if File.query.filter_by(path=str(jsonpathrel)).first():
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
        filedateformatalt = "%Y%m%d%H%M%S"
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
                # sometimes, the name still contains a date. yes, really
                # more fun: the second date is usually more accurate (??!)
                try:
                    date, name = name.split('-', 1)
                    date = datetime.datetime.strptime(date, filedateformatalt)
                except ValueError:
                    pass
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
        try:
            im = PIL.Image.open(datapath)
        except Exception:
            pass
        if im and 'ImageNavigation' in data:
            try:
                proj = Projection.from_nav(im.size[0], im.size[1], data['ImageNavigation'])
                if proj:
                    proj = proj.find_or_insert()
            except KeyError:
                proj = None

        common = dict(
            meta = data,
            date = date,
            name = name,
            source = source.lower(),
        )
        if im:
            if proj:
                style = MapStyle.NORMAL
                if channel.lower().endswith('_enhanced'):
                    style = MapStyle.ENHANCED
                    channel = channel.rsplit('_', 1)[0]
                if channel.lower() == 'fc':
                    style = MapStyle.FALSECOLOR

                newprod = MapProduct(
                    width=im.size[0],
                    height=im.size[1],
                    region=region.lower(),
                    channel=channel.lower(),
                    projection=proj,
                    style=style,
                    **common
                )
            else:
                newprod = ImageProduct(
                    width=im.size[0],
                    height=im.size[1],
                    **common
                )
        else:
            # try to detect an awips nnn, xxx
            nnns = goesbrowse.application.get_awips_nnn()
            if len(common['name']) >= 5:
                nnn = common['name'][:3].lower()
                xxx = common['name'][3:].lower()
                if nnn in nnns:
                    newprod = TextProduct(**common, nnn=nnn, xxx=xxx)
                else:
                    newprod = TextProduct(**common)

        sql.session.add(newprod)

        main = File(
            path = str(datapathrel),
            size = datasize,
            type = FileType.MAIN,
            product = newprod,
        )

        meta = File(
            path = str(jsonpathrel),
            size = jsonsize,
            type = FileType.META,
            product = newprod,
        )

        sql.session.add(main)
        sql.session.add(meta)

        if self.thumbnail and im is not None:
            thumbpath = datapath.with_suffix('.thumbnail.' + suffix)
            thumbpathrel = thumbpath.relative_to(self.root)
            print('generating', thumbpathrel)
            im.thumbnail((self.thumbnail, self.thumbnail))
            im.save(str(thumbpath))

            thumb = File(
                path = str(thumbpathrel),
                size = thumbpath.stat().st_size,
                type = FileType.THUMBNAIL,
                product = newprod,
            )
            sql.session.add(thumb)
