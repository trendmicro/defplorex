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

import logging

from celeryapp import app as clapp

from defplorex.transformer import TagTransformer, TransformerFactory, Pipeline

log = logging.getLogger(__name__)


class ProcessorTask(object):
    max_retries = 3
    default_retry_delay = 30

    _settings = None
    _es = None

    @property
    def settings(self):
        if self._settings is None:
            from config import load_settings
            self._settings = load_settings()
        return self._settings

    @property
    def es(self):
        if self._es is None:
            from defplorex.backend.elastic import ES
            self._es = ES(self.settings)
        return self._es

    def __init__(self, transformers, tr_args=[], tr_kwargs={}):
        self.transformers = [TagTransformer()]

        if isinstance(transformers, list):
            for k in transformers:
                tr_kwargs.update(**dict(settings=self.settings))
                self.transformers.append(k(*tr_args, **tr_kwargs))

    def run(self, ids, index, *args, **kwargs):
        log.info('Received task for %d IDs on index %s', len(ids), index)

        query = dict(query=dict(ids=dict(values=filter(lambda x: x, ids))))
        kwargs.update(**dict(settings=self.settings))
        update = kwargs.get('update', True)
        ephemeral = kwargs.get('ephemeral', False)

        def _transform(doc):
            return Pipeline.chain(
                    doc,
                    self.transformers,
                    updates_only=update, *args, **kwargs)

        if ephemeral:
            return [_transform(doc) for doc in self.es.scan(index, query)]

        err_ids = self.es.partial_update_from_query(
                index=index,
                query=query,
                transform=_transform)

        if err_ids:
            raise Exception('IDs = %s have failed (will retry)', err_ids)


@clapp.task(
        bind=True,
        default_retry_delay=ProcessorTask.default_retry_delay,
        max_retries=ProcessorTask.max_retries)
def processor_task(self, ids, index, **kwargs):
    """
    Generic task that executes a serie of transformations on the doc
    """
    transformers_lst = kwargs.get('transformers_lst', [])
    tr_args = kwargs.get('tr_args', [])
    tr_kwargs = kwargs.get('tr_kwargs', {})
    ephemeral = kwargs.get('ephemeral', False)

    if index is None:
        return []

    transformers = TransformerFactory.get_by_list(transformers_lst)
    processor = ProcessorTask(
            transformers,
            tr_args=tr_args,
            tr_kwargs=tr_kwargs)

    try:
        r = processor.run(ids, index, **kwargs)
        if ephemeral:
            return r
    except Exception as e:
        log.warn('Retrying task %s because: %s', self.request.id, e)
        raise self.retry(exc=e)
