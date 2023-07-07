import logging


class loggers:
    """

    Class to handle loggigng

    """

    def __init__(self, log_names):
        """Initialize logging objects"""
        self._loggers = []
        self._level = logging.ERROR
        for nm in log_names:
            self._loggers.append(logging.getLogger(nm))

    def set_default_level(self, lev):
        """
        Set the default level to log

        lev - Log Level to put to screen
        """
        self._level = lev
        self.set_log_level()

    def set_log_level(self):
        """

        Set the log_level on all loggers

        """
        for log in self._loggers:
            log.setLevel(self._level)

    def add_logger(self, log_name):
        """
        Add a logger


        log_name - Logger name

        """
        self._loggers.append(logging.getLogger(log_name))
        self.set_log_level()


sep_loggers = loggers(["generic_solver"])
sep_loggers.set_default_level(logging.ERROR)
