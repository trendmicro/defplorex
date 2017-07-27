# -*- coding: utf-8 -*-

# Copyright (c) 2017, Trend Micro Incorporated
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are
# those of the authors and should not be interpreted as representing official
# policies, either expressed or implied, of the FreeBSD Project.

from __future__ import division

import re
import logging

from progress.bar import Bar
from progress.spinner import Spinner
from progress.helpers import WriteMixin
from progress import Infinite
import humanize

ip_re = re.compile(
        '(([2][5][0-5]\.)|([2][0-4][0-9]\.)|([0-1]?[0-9]?[0-9]\.)){3}'
        '(([2][5][0-5])|([2][0-4][0-9])|([0-1]?[0-9]?[0-9]))')


log = logging.getLogger(__name__)


def fopen(fname, *args):
    if not fname:
        import sys
        return sys.stdout

    if fname.endswith('.gz'):
        import gzip
        return gzip.open(fname, *args)
    return open(fname, *args)


class Counter(WriteMixin, Infinite):
    message = ''

    def __init__(self, name):
        super(Counter, self).__init__()
        self.name = name

    def update(self, txt):
        self.write('{}: {}'.format(self.name, str(txt)))


class FancyBar(Bar):
    message = ''
    fill = '*'
    suffix = '[%(percent)d%%] %(index)d/%(max)d - ' \
             'ETA: %(eta)ds' \
             ' (%(elapsed_td)s) - %(avg)f sec/itm'


class SlowFancyBar(Bar):
    message = ''
    fill = '*'
    suffix = '[%(percent)d%%] %(index)d/%(max)d - ' \
             'ETA: %(eta)ds ~= %(rem_h)dhrs' \
             ' (%(elapsed_td)s) - %(avg)f s/itm'

    @property
    def rem_h(self):
        return self.eta // 3600


class SlowOverallFancyBar(Bar):
    message = ''
    fill = '*'
    suffix = '[%(percent)d%%] %(index)d/%(max)d ' \
             'ETA: %(natural_eta)s' \
             ' (%(natural_overall_eta)s for %(grand_tot)s)' \
             ' (%(nat_elapsed)s) - %(avg)f s/itm'

    def __init__(self, *args, **kwargs):
        self.grand_total = kwargs.pop('grand_total')
        super(SlowOverallFancyBar, self).__init__(*args, **kwargs)

    @property
    def natural_eta(self):
        return humanize.naturaldelta(self.eta)

    @property
    def natural_overall_eta(self):
        return humanize.naturaldelta(self.avg * self.grand_total)

    @property
    def grand_tot(self):
        return humanize.intword(self.grand_total)

    @property
    def nat_elapsed(self):
        return humanize.naturaldelta(self.elapsed_td)


class FancySpinner(Spinner):
    suffix = '%(index)d'
