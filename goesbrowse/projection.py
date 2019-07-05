import math

import attr

# The math in this code was copied from the PROJ.4 source. PROJ.4 is a
# crazy amazing piece of work, and you should check it out. PROJ.4 is
#
# Copyright (c) 2000, Frank Warmerdam
#
# and licensed under the MIT.
#
# Any bugs, though, are surely my fault.

# Some references:
#
# Ellipsoids: https://github.com/OSGeo/PROJ/blob/0ea2b4e82700ba9aa0ae67ec0ee72ae58bc4f0c9/src/ell_set.cpp#L495
# Named Models: https://github.com/OSGeo/PROJ/blob/0ea2b4e82700ba9aa0ae67ec0ee72ae58bc4f0c9/src/ellps.cpp
# Ellipsoid parameters: https://github.com/OSGeo/PROJ/blob/0ea2b4e82700ba9aa0ae67ec0ee72ae58bc4f0c9/src/ell_set.cpp#L256
# Geostationary Projection: https://github.com/OSGeo/PROJ/blob/8ab6f683cd316acf57bb89ed83932a267c5aa3c2/src/projections/geos.cpp

# constant scale for GEOS satellite image projection
# this is a prefactor for ColumnScaling and LineScaling
# I have NO IDEA why it's this value. I just hand-tuned it.
# If somebody could explain this, that would be great.
SCALE_FACTOR = 0.0001557991315541723

@attr.s
class Ellipsoid:
    a = attr.ib() # semimajor axis
    es = attr.ib() # eccentricity squared

    def __attrs_post_init__(self):
        self.one_es = 1 - self.es
        self.rone_es = 1 / self.one_es

    NAMES = {
        'GRS80': dict(a=6378137.0, rf=298.257222101, desc='GRS 1980(IUGG, 1980)'),
    }

    @classmethod
    def from_params(cls, a, **other):
        if not other:
            return cls(a=a, es=0)

        if 'rf' in other: # reverse flattening
            f = 1 / other['rf']
            es = 2 * f - f ** 2
        elif 'f' in other: # flattening
            es = 2 * other['f'] - other['f'] ** 2
        elif 'es' in other: # eccentricity squared
            es = other['es']
        elif 'e' in other: # eccentricity
            es = other['e'] ** 2
        elif 'b' in other: # semiminor axis
            es = 1 - (other['b'] ** 2) / (other['a'] ** 2)
        else:
            raise ValueError('unknown shape parameter')

        return cls(a=a, es=es)

    @classmethod
    def from_sphere(cls, r=6371008.8):
        return cls.from_params(a=r)

    @classmethod
    def from_name(cls, name):
        info = cls.NAMES[name]
        return cls.from_params(**info)

@attr.s
class GeosProj:
    h = attr.ib()
    sweep = attr.ib(default='y')
    lon_0 = attr.ib(default=0.0)
    R = attr.ib(default=None)
    ellps = attr.ib(default='GRS80')

    @h.validator
    def _check_h(self, attribute, value):
        if value < 0:
            raise ValueError('negative h')

    @sweep.validator
    def _check_sweep(self, attribute, value):
        if not value in ['x', 'y']:
            raise ValueError('bad sweep axis: {}'.format(value))

    def __attrs_post_init__(self):
        self.flip_axis = {'x': True, 'y': False}[self.sweep]

        if self.R:
            self.model = Ellipsoid.from_sphere(self.R)
        elif isinstance(self.ellps, Ellipsoid):
            self.model = self.ellps
        else:
            self.model = Ellipsoid.from_name(self.ellps)

        self.radius_g_1 = self.h / self.model.a
        self.radius_g = 1 + self.radius_g_1
        self.C = self.radius_g ** 2 - 1
        if self.model.es != 0:
            self.radius_p = math.sqrt(self.model.one_es)
            self.radius_p2 = self.model.one_es
            self.radius_p_inv2 = self.model.rone_es
        else:
            self.radius_p = self.radius_p2 = self.radius_p_inv2 = 1

    def forward(self, lam, phi):
        lam -= math.radians(self.lon_0)
        phi = math.atan(self.radius_p2 * math.tan(phi))

        r = self.radius_p / math.sqrt((self.radius_p * math.cos(phi)) ** 2 + math.sin(phi) ** 2)
        Vx = r * math.cos(lam) * math.cos(phi)
        Vy = r * math.sin(lam) * math.cos(phi)
        Vz = r * math.sin(phi)

        tmp = self.radius_g - Vx
        if tmp * Vx - Vy * Vy - Vz * Vz * self.radius_p_inv2 < 0:
            return None

        if self.flip_axis:
            x = self.radius_g_1 * math.atan(Vy / math.sqrt(Vz ** 2 + tmp ** 2))
            y = self.radius_g_1 * math.atan(Vz / tmp)
        else:
            x = self.radius_g_1 * math.atan(Vy / tmp)
            y = self.radius_g_1 * math.atan(Vz / math.sqrt(Vy ** 2 + tmp ** 2))

        return x, y

    def reverse(self, x, y):
        Vx = -1
        if self.flip_axis:
            Vz = math.tan(y / self.radius_g_1)
            Vy = math.tan(x / self.radius_g_1) * math.sqrt(1 + Vz ** 2)
        else:
            Vy = math.tan(x / self.radius_g_1)
            Vz = math.tan(y / self.radius_g_1) * math.sqrt(1 + Vy ** 2)

        a = Vx ** 2 + Vy ** 2 + (Vz / self.radius_p) ** 2
        b = 2 * self.radius_g * Vx
        det = b ** 2 - 4 * a * self.C
        if det < 0:
            return None

        k = (-b - math.sqrt(det)) / (2 * a)
        Vx = self.radius_g + k * Vx
        Vy *= k
        Vz *= k

        lam = math.atan2(Vy, Vx)
        phi = math.atan(Vz * math.cos(lam) / Vx)
        phi = math.atan(self.radius_p_inv2 * math.tan(phi))

        lam += math.radians(self.lon_0)

        return lam, phi
