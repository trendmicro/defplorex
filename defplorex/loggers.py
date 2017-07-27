# -*- coding: utf-8 -*-

# copyright (c) 2017, trend micro incorporated
# all rights reserved.
#
# redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# 2. redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# this software is provided by the copyright holders and contributors "as is"
# and any express or implied warranties, including, but not limited to, the
# implied warranties of merchantability and fitness for a particular purpose
# are disclaimed. in no event shall the copyright owner or contributors be
# liable for any direct, indirect, incidental, special, exemplary, or
# consequential damages (including, but not limited to, procurement of
# substitute goods or services; loss of use, data, or profits; or business
# interruption) however caused and on any theory of liability, whether in
# contract, strict liability, or tort (including negligence or otherwise)
# arising in any way out of the use of this software, even if advised of the
# possibility of such damage.
#
# the views and conclusions contained in the software and documentation are
# those of the authors and should not be interpreted as representing official
# policies, either expressed or implied, of the freebsd project.

import logging
import logging.config
import logging.handlers
import socket

import arrow
from tzlocal import get_localzone

from defplorex.config import load_settings

log = logging.getLogger(__name__)


class Formatter(logging.Formatter):
    """Simple formatter"""

    def converter(self, timestamp):
        """Convert date to local timezone"""

        ltz = get_localzone()
        converted = arrow.get(
                timestamp,
                tz=ltz).to('UTC')
        return converted.datetime.timetuple()


def config_logger(level=logging.WARN, debug=False):
    """Configure logger"""

    # settings
    settings = load_settings()
    project = settings.get('project', 'project')
    host = socket.getfqdn()
    _logging = settings.get('LOGGING').copy()

    if settings.get('DEBUG', False) or debug:
        level = logging.DEBUG

    # override level if DEBUG
    for handler in _logging.get('handlers').keys():
        _logging['handlers'][handler]['level'] = level
    for logger in _logging.get('loggers').keys():
        _logging['loggers'][logger]['level'] = level
    if 'root' in _logging:
        _logging['root']['level'] = level

    # set host in format
    if 'logstash' in _logging.get('formatters'):
        fmt = _logging['formatters']['logstash']['format']
        fmt = fmt.format(host=host, project=project)
        _logging['formatters']['logstash']['format'] = fmt

    if 'logstash' in _logging.get('handlers'):
        address = _logging['handlers']['logstash']['address']
        _logging['handlers']['logstash']['address'] = tuple(address)

    logging.config.dictConfig(_logging)

    log.info('Logger configured: %s', log)


if __name__ == '__main__':
    config_logger()
