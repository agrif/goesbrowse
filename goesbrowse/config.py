import os
import pathlib
import sys

import attr
import humanfriendly
import toml

@attr.s
class Config:
    database = attr.ib(default=None)
    files = attr.ib(default=None)
    quota = attr.ib(default=0)
    use_x_accel_redirect = attr.ib(default=None)

    def set_if_present(self, data, k, trans=lambda x: x):
        if k in data:
            setattr(self, k, trans(data.pop(k)))
    
    def merge(self, data, root):
        self.set_if_present(data, 'database', lambda x: (root / pathlib.Path(x).expanduser()).resolve())
        self.set_if_present(data, 'files', lambda x: (root / pathlib.Path(x).expanduser()).resolve())

        self.set_if_present(data, 'quota')
        if not isinstance(self.quota, int):
            self.quota = humanfriendly.parse_size(self.quota)

        self.set_if_present(data, 'use_x_accel_redirect')

    @classmethod
    def load_file(cls, path, merge=None):
        root = path.parent
        with open(str(path)) as f:
            data = toml.load(f)

        if merge is None:
            merge = cls()

        for path in data.pop('inherit', []):
            cls.load_file(root / path, merge=merge)

        merge.merge(data, root)
        if data:
            raise RuntimeError('unknown config keys: {}'.format(list(data.keys())))
        return merge

def discover(extras=[]):
    paths = extras + [os.environ.get('GOESBROWSE'), '~/.goesbrowse.toml']
    paths = [pathlib.Path(p).expanduser() for p in paths if p]
    paths = [p for p in paths if p.exists()]
    
    for path in paths:
        return Config.load_file(path)
    raise RuntimeError('no config found')

if __name__ == '__main__':
    v = discover(sys.argv[1:])
    print(v)

