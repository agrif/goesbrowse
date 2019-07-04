import click
import flask

import goesbrowse.web

@click.group(cls=flask.cli.FlaskGroup, create_app=lambda scriptinfo: goesbrowse.web.app)
@click.option('--config')
def cli(config):
    goesbrowse.web.app.config['configpath'] = config

@cli.command()
def update():
    appdb = goesbrowse.web.get_db()
    appdb.update()

@cli.command()
@click.option('--dry-run', '-n', is_flag=True)
def clean(dry_run):
    appdb = goesbrowse.web.get_db()
    appdb.clean(dry_run=dry_run)

@cli.command()
def timelapse():
    appdb = goesbrowse.web.get_db()
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
