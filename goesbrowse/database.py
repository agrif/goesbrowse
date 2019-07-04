import datetime
import json
import pathlib
import sqlite3

import dateutil.tz

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
        size = self.query_one('select sum(size) from goesfiles {}'.format(where), *whereargs)[0]
        if size is None:
            size = 0
        return size

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
        if not self.quota:
            return

        excess = self.size - self.quota
        if excess <= 0:
            return

        # fixme: https://gist.github.com/Gizmokid2005/2bb9cc3746f4f0ea0dbfb83e7d64a8da
        for file in self.query('select rowid, size, datapath, jsonpath from goesfiles order by date'):
            excess -= file['size']
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
        self.commit()

    def clean(self, dry_run=False):
        for file in list(self.get_above_quota()):
            print('deleting', file['datapath'])
            if not dry_run:
                self.query('delete from goesfiles where rowid = ?', file['rowid'])
                (self.root / file['datapath']).unlink()
                (self.root / file['jsonpath']).unlink()
        if not dry_run:
            self.commit()
        self.remove_empty_dirs(self.root, dry_run=dry_run)

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
