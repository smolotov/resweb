from flask import Flask, Blueprint, g, redirect, request, url_for
from pyres import ResQ, failure

from resweb.views import (
    Overview,
    Queues,
    Queue,
    Workers,
    Working,
    Failed,
    Stats,
    Stat,
    Worker,
    Delayed,
    DelayedTimestamp
)
from base64 import b64decode

app = Flask(__name__)
app.config.from_object('resweb.default_settings')
app.config.from_envvar('RESWEB_SETTINGS', silent=True)
mod = Blueprint('resweb', __name__, static_folder='static')

@mod.before_request
def before_request():
    """Make sure we are connected to the database each request."""
    g.pyres = ResQ(app.config['RESWEB_HOST'], password=app.config.get('RESWEB_PASSWORD', None))

@mod.teardown_request
def teardown_request(exception):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'pyres'):
        g.pyres.close()

@mod.route("/")
def overview():
    return Overview(g.pyres).render().encode('utf-8')

@mod.route("/working/")
def working():
    return Working(g.pyres).render().encode('utf-8')

@mod.route("/queues/")
def queues():
    return Queues(g.pyres).render().encode('utf-8')

@mod.route('/queues/<queue_id>/')
def queue(queue_id):
    start = int(request.args.get('start', 0))
    return Queue(g.pyres, queue_id, start).render().encode('utf-8')

@mod.route('/failed/')
def failed():
    start = request.args.get('start', 0)
    start = int(start)
    return Failed(g.pyres, start).render().encode('utf-8')

@mod.route('/failed/retry/', methods=["POST"])
def failed_retry():
    failed_job = request.form['failed_job']
    job = b64decode(failed_job)
    decoded = ResQ.decode(job)
    failure.retry(g.pyres, decoded['queue'], job)
    return redirect(url_for('.failed'))

@mod.route('/failed/delete/', methods=["POST"])
def failed_delete():
    failed_job = request.form['failed_job']
    job = b64decode(failed_job)
    failure.delete(g.pyres, job)
    return redirect(url_for('.failed'))

@mod.route('/failed/delete_all/')
def delete_all_failed():
    #move resque:failed to resque:failed-staging
    g.pyres.redis.rename('resque:failed', 'resque:failed-staging')
    g.pyres.redis.delete('resque:failed-staging')
    return redirect(url_for('.failed'))


@mod.route('/failed/retry_all/')
def retry_failed(number=5000):
    failures = failure.all(g.pyres, 0, number)
    for f in failures:
        j = b64decode(f['redis_value'])
        failure.retry(g.pyres, f['queue'], j)
    return redirect(url_for('.failed'))

@mod.route('/workers/<worker_id>/')
def worker(worker_id):
    return Worker(g.pyres, worker_id).render().encode('utf-8')

@mod.route('/workers/')
def workers():
    return Workers(g.pyres).render().encode('utf-8')

@mod.route('/stats/')
def stats_resque():
    return redirect(url_for('.stats', key="resque"))

@mod.route('/stats/<key>/')
def stats(key):
    return Stats(g.pyres, key).render().encode('utf-8')

@mod.route('/stat/<stat_id>/')
def stat(stat_id):
    return Stat(g.pyres, stat_id).render().encode('utf-8')

@mod.route('/delayed/')
def delayed():
    start = request.args.get('start', 0)
    start = int(start)
    return Delayed(g.pyres, start).render().encode('utf-8')

@mod.route('/delayed/<timestamp>/')
def delayed_timestamp(timestamp):
    start = request.args.get('start', 0)
    start = int(start)
    return DelayedTimestamp(g.pyres, timestamp, start).render().encode('utf-8')

def main():
    app.register_blueprint(mod)

    app.run(host=app.config['SERVER_HOST'], port=app.config['SERVER_PORT'], debug=True)
if __name__ == "__main__":
    main()
