function mapSetup(imageId, canvasId, metaUrl, geoUrls) {
    var image = document.getElementById(imageId);
    var canvas = document.getElementById(canvasId);
    var geo = [];
    fetch(metaUrl).then(function(response) {
        return response.json();
    }).then(function(meta) {
        function fetchNextGeo() {
            if (geoUrls.length <= 0)
                return;
            var next = geoUrls.shift();
            return fetch(next).then(function(response) {
                return response.json();
            }).then(function(data) {
                geo.push(data);
                return fetchNextGeo();
            })
        }

        fetchNextGeo().then(function() {
            var lon_0 = -75.0; // FIXME
            var proj = makeGeosProj(35786023.0, true, lon_0);

            function resize(redraw) {
                canvas.style.position = "absolute";
                canvas.style.left = image.offsetLeft + "px";
                canvas.style.top = image.offsetTop + "px";
                canvas.style.width = image.offsetWidth;
                canvas.style.height = image.offsetHeight;

                canvas.width = image.offsetWidth;
                canvas.height = image.offsetHeight;

                if (redraw) {
                    var ctx = canvas.getContext("2d");
                    ctx.save();
                    ctx.scale(image.offsetWidth / image.naturalWidth, image.offsetHeight / image.naturalHeight);
                    mapRedraw(image, ctx, meta, proj, geo);
                    ctx.restore();
                }
            }

            function redraw() {
                resize(true);
            }

            var resizeTimer;
            window.addEventListener("resize", function() {
                clearTimeout(resizeTimer);
                resizeTimer = setTimeout(function() {
                    redraw();
                }, 250);
                resize();
            });
            redraw();
        });
    });
}

function makeEllipsoid(a, es) {
    var obj = {
        a: a,
        es: es,
    };
    obj.one_es = 1 - es;
    obj.rone_es = 1 / obj.one_es;
    return obj;
}

function makeEllipsoidRf(a, rf) {
    var f = 1 / rf;
    var es = 2 * f - f * f;
    return makeEllipsoid(a, es);
}

function radians(degrees) {
    return degrees * Math.PI / 180;
}

function makeGeosProj(h, flip, lon_0) {
    var obj = {
        h: h,
        flip: flip,
        lon_0: lon_0,
    };

    // GRS80
    obj.model = makeEllipsoidRf(6378137.0, 298.257222101);

    obj.radius_g_1 = obj.h / obj.model.a;
    obj.radius_g = 1 + obj.radius_g_1;
    obj.C = obj.radius_g * obj.radius_g - 1;
    
    obj.radius_p = Math.sqrt(obj.model.one_es);
    obj.radius_p2 = obj.model.one_es;
    obj.radius_p_inv2 = obj.model.rone_es;

    obj.forward = function(lam, phi) {
        lam -= radians(obj.lon_0);
        phi = Math.atan(obj.radius_p2 * Math.tan(phi));

        var r = obj.radius_p / Math.sqrt(Math.pow(obj.radius_p * Math.cos(phi), 2) + Math.pow(Math.sin(phi), 2));
        var Vx = r * Math.cos(lam) * Math.cos(phi);
        var Vy = r * Math.sin(lam) * Math.cos(phi);
        var Vz = r * Math.sin(phi);

        var tmp = obj.radius_g - Vx;
        if (tmp * Vx - Vy * Vy - Vz * Vz * obj.radius_p_inv2 < 0)
            return null;

        var x, y;
        if (obj.flip) {
            x = obj.radius_g_1 * Math.atan(Vy / Math.sqrt(Vz * Vz + tmp * tmp));
            y = obj.radius_g_1 * Math.atan(Vz / tmp)
        } else {
            x = obj.radius_g_1 * Math.atan(Vy / tmp)
            y = obj.radius_g_1 * Math.atan(Vz / Math.sqrt(Vy * Vy + tmp * tmp));
        }

        return [x, y];
    };

    obj.reverse = function(x, y) {
        // FIXME
    };

    return obj;
}

function mapRedraw(image, ctx, meta, proj, geo) {
    ctx.strokeStyle = '#FFFFFF';
    ctx.lineWidth = 3;
    ctx.globalCompositeOperation = 'xor';
    ctx.globalAlpha = 0.5;

    // ???
    var magic = 0.0001557991315541723;
    var nav = meta['ImageNavigation'];
    function projToCtx(pt) {
        var x = nav['ColumnOffset'] + nav['ColumnScaling'] * magic * pt[0];
        var y = nav['LineOffset'] - nav['LineScaling'] * magic * pt[1];
        return [x, y];
    }

    function drawPoly(poly) {
        ctx.beginPath();
        for (var i = 0; i < poly.length; i++) {
            var a = poly[i];

            var lam = radians(a[0]);
            var phi = radians(a[1]);

            var axy = proj.forward(lam, phi);
            if (axy) {
                axy = projToCtx(axy);
                if (i == 0)
                    ctx.moveTo(axy[0], axy[1]);
                else
                    ctx.lineTo(axy[0], axy[1]);
            }
        }
        ctx.closePath();

        ctx.save()
        ctx.resetTransform();
        ctx.stroke();
        ctx.restore();
    }

    geo.forEach(function(geofile) {
        geofile['features'].forEach(function(feature) {
            var geom = feature['geometry'];
            if (geom['type'] == 'Polygon') {
                geom['coordinates'].forEach(function(poly) {
                    drawPoly(poly);
                });
            } else if (geom['type'] == 'MultiPolygon') {
                geom['coordinates'].forEach(function(multi) {
                    multi.forEach(function(poly) {
                        drawPoly(poly);
                    });
                });
            }
        });
    });
}
