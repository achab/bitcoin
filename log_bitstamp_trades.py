import pusherclient
import time
import logging
import sys
import datetime
import signal
import os

logging.basicConfig()
log_file_fd = None

def sigint_and_sigterm_handler(signal, frame):
    global log_file_fd
    log_file_fd.close()
    sys.exit(0)

class BitstampLogger:

    def __init__(self, log_file_path, log_file_reload_path, pusher_key, channel, event):
        self.channel = channel
        self.event = event
        self.log_file_fd = open(log_file_path, "a")
        self.log_file_reload_path = log_file_reload_path
        self.pusher = pusherclient.Pusher(pusher_key)
        self.pusher.connection.logger.setLevel(logging.WARNING)
        self.pusher.connection.bind('pusher:connection_established', self.connect_handler)
        self.pusher.connect()

    def callback(self, data):
        utc_timestamp = time.mktime(datetime.datetime.utcnow().timetuple())
        line = str(utc_timestamp) + " " + data + "\n"
        if os.path.exists(self.log_file_reload_path):
            os.remove(self.log_file_reload_path)
            self.log_file_fd.close()
            self.log_file_fd = open(log_file_path, "a")
        self.log_file_fd.write(line)

    def connect_handler(self, data):
        channel = self.pusher.subscribe(self.channel)
        channel.bind(self.event, self.callback)


def main(log_file_path, log_file_reload_path):
    global log_file_fd
    bitstamp_logger = BitstampLogger(
        log_file_path,
        log_file_reload_path,
        "de504dc5763aeef9ff52",
        "live_trades",
        "trade")
    log_file_fd = bitstamp_logger.log_file_fd
    signal.signal(signal.SIGINT, sigint_and_sigterm_handler)
    signal.signal(signal.SIGTERM, sigint_and_sigterm_handler)
    while True:
        time.sleep(1)

if __name__ == "__main__":
    log_file_path = sys.argv[1]
    log_file_reload_path = sys.argv[2]
    main(log_file_path, log_file_reload_path)
