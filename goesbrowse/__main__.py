import click
import flask

import goesbrowse.application
import goesbrowse.database

@click.group(cls=flask.cli.FlaskGroup, create_app=lambda scriptinfo: goesbrowse.application.app)
@click.option('--config')
def cli(config):
    goesbrowse.application.app.config['GOESBROWSE_CONFIG_PATH'] = config
    with goesbrowse.application.app.app_context():
        goesbrowse.application.get_db()

@cli.command()
def update():
    appdb = goesbrowse.application.get_db()
    appdb.update()

@cli.command()
@click.option('--dry-run', '-n', is_flag=True)
def clean(dry_run):
    appdb = goesbrowse.application.get_db()
    appdb.clean(dry_run=dry_run)

@cli.command()
def timelapse():
    conf = goesbrowse.application.get_config()
    appdb = goesbrowse.application.get_db()
    # ffmpeg -f concat -i fstest.txt -vf "fps=10, scale=512:-1, drawtext=text='%{metadata\:imagedate}': fontcolor=0xaaaaaa: font=mono: fontsize=14: x=10: y=h-th-10" -pix_fmt yuv420p -c:v libx264 -preset veryslow -crf 19 -profile:v high -level 4.2 -movflags +faststart output.mp4
    postroll = 5
    rate = 1 / (8 * 60 * 60)

    prods = goesbrowse.database.MapProduct.query.filter_by(region='fd', source='goes16', channel='fc').order_by(goesbrowse.database.Product.date)
    lastdate = None
    print('ffconcat version 1.0')
    for p in prods:
        if conf.thumbnail:
            f = p.get_file('THUMBNAIL')
        else:
            f = p.get_file('MAIN')
        if not f:
            continue
        if lastdate is not None:
            duration = p.localdate - lastdate
            print('duration {}'.format(duration.total_seconds() * rate))
        print('file {}'.format(f.path))
        print('file_packet_metadata imagedate=\'{:%a %b %d %Y, %H:%M:%S %Z}\''.format(p.localdate))
        lastdate = p.localdate
    print('duration {}'.format(postroll))

if __name__ == '__main__':
    cli()
