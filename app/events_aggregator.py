from argparse import ArgumentParser
import json
import logging
import os
import signal
import sys
import time
from bson import json_util
import tornado.escape
import tornado.web
import tornado.ioloop
import motor
from pipelines import aggregate_events


PROG_NAME = os.path.basename(sys.argv[0])

# Sampling resolutions
# This dictionary may be extended in future
TIME_RESOLUTIONS = {
    "hour": 3600,
    "day": 86400,
}


logging.basicConfig()
logger = logging.getLogger(__name__)

# Methods decorated with @tornado.web.asynchronous are "asynchronous"
# because they exit before the HTTP request completes.


class JSONRequestHandler(tornado.web.RequestHandler):
    """
    This is a convenient base class which brings the common
    methods used by both /events and /stats handlers.
    """

    def prepare(self):
        """
        Check whether the content type is JSON in case of a
        POST request. Conversely, it does not matter in case
        of GET as they are not defined in the specifications
        and the handlers are provided debugging purposes only.
        """
        content_type = self.request.headers.get(
            'Content-Type', 'application/x-www-form-urlencoded',
        )
        method = self.request.method
        if method == 'POST':
            if content_type.startswith("application/json"):
                self.json_args = json.loads(self.request.body)
            else:
                msg = "This service accepts JSON requests only"
                logger.error(msg)
                raise tornado.web.HTTPError(400, msg)
        else:
            logging.warning(
                "Other requests than POST are not part of the "
                "interface specifications. Use at your own risk."
            )

    def _round_time(self, time, resolution):
        """Round time to nearest hour or day according to resolution."""
        return int(time - (time % resolution))

    def _now(self):
        """Convenience method that returns the current time."""
        return int(time.time())


class EventsHandler(JSONRequestHandler):

    @tornado.web.asynchronous
    def post(self):
        data = self.json_args.copy()
        logger.debug("Inserting: %s", data)
        self.settings['db'].events.insert(
            ({
                "type": data["type"],
                "time": timestamp,
                "object_id": data["object_id"],
            } for timestamp in self._prepare_timestamps_for_insert(
                int(data["time"]),
            )),
            callback=self._on_post_response,
        )

    @tornado.web.asynchronous
    def get(self):
        self.settings['db'].events.find().sort(
            [('time', -1)]
        ).to_list(
            self.settings["max_items"],
            callback=self._on_get_response,
        )

    def _on_get_response(self, result, error):
        if error:
            raise tornado.web.HTTPError(500, error)
        self.set_status(200)
        self.write(
            json_util.dumps(result)
        )
        self.finish()

    def _on_post_response(self, result, error):
        if error:
            raise tornado.web.HTTPError(500, error)
        self.set_status(200)
        self.finish()

    def _prepare_timestamps_for_insert(self, time):
        for resolution, seconds in TIME_RESOLUTIONS.items():
            round_time = self._round_time(time, seconds)
            logger.debug(
                "Normalizing timestamp '%s' -> '%s': '%s'",
                time, resolution, round_time,
            )
            yield round_time


class StatsHandler(JSONRequestHandler):

    @tornado.web.asynchronous
    def post(self):
        data = self.json_args.copy()
        object_id = int(data["object_id"])
        start, end = self._compute_start_and_end(
            TIME_RESOLUTIONS[data['resolution']],
            int(data['limit']),
        )

        pipeline = aggregate_events(object_id, start, end)
        self.settings['db'].events.aggregate(
            pipeline,
        ).to_list(
            self.settings["max_items"],
            callback=self._on_post_response,
        )

    def _on_post_response(self, result, error):
        if error:
            raise tornado.web.HTTPError(500, error)
        else:
            response = {
                obj["type"]: obj["events"] for obj in result
            }
            self.write(
                json.dumps(response)
            )
        self.finish()

    def _compute_start_and_end(self, resolution, limit):
        now = self._round_time(self._now(), resolution)
        start = now - (resolution * limit)
        return start, now


def quit():
    tornado.ioloop.IOLoop.instance().stop()


def sig_handler(sig, frame):
    if sig == signal.SIGINT:
        sig = 'SIGINT'
    elif sig == signal.SIGTERM:
        sig = 'SIGTERM'
    print >>sys.stderr, "Caught %s, exiting." % sig
    tornado.ioloop.IOLoop.instance().add_callback(quit)


def make_app(db, debug, max_items, port):
    application = tornado.web.Application(
        [
            (r"/event", EventsHandler),
            (r"/stats", StatsHandler),
        ],
        db=db,
        debug=debug,
        max_items=max_items,
    )
    application.listen(port)
    return application


def main():
    parser = ArgumentParser(
        prog=PROG_NAME,
        description="Simple events aggregator",
    )
    parser.add_argument(
        "-D", "--debug",
        action="store_true",
        dest="debug",
        help="Enable debug mode. Also enables extra verbose output.",
        default=None,
    )
    parser.add_argument(
        "--database-uri",
        action="store",
        dest="database_uri",
        help="MongoDB URI. Specify database's name separately.",
        default=None,
    )
    parser.add_argument(
        "--database-name",
        action="store",
        dest="database_name",
        help="Database name (default: 'test')",
        default="test",
    )
    parser.add_argument(
        "-P", "--port",
        action="store",
        dest="port",
        type=int,
        help="Server's post (default: 8888)",
        default=8888,
    )
    parser.add_argument(
        "--max-items",
        action="store",
        dest="max_items",
        type=int,
        help="Maximum number of items that can be "
             "returned by a query (default: 9999999999)",
        default=9999999999,
    )
    args = parser.parse_args()

    # Set up logging
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    # Set up the datastore
    db = motor.MotorClient(args.database_uri)[args.database_name]
    # Initialize Tornado application
    make_app(db, args.debug, args.max_items, args.port)

    logger.info("Listening on http://localhost:%s", args.port)
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()
