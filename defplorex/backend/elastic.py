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

# built-in modules
import gc
import logging
from datetime import datetime

# 3rd party modules
import simplejson
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search, Q

log = logging.getLogger(__name__)


class FailedTransformException(Exception):
    def __init__(self, message, _id):
        super(FailedTransformException, self).__init__(message)

        self._id = _id


class ESStorer(object):
    """
    Generic ES wrapper
    """
    def __init__(self, settings):
        kwargs = settings.get('es').get('client')
        es_user = settings.get('es_user')
        es_pass = settings.get('es_pass')

        if es_user and es_pass:
            kwargs.update(**dict(http_auth=(es_user, es_pass)))

        self.client = Elasticsearch(**kwargs)
        self.timeout = settings.get('es').get('client').get('timeout')
        self.doc_type = settings.get('es').get('doc_type')
        self.index_name = settings.get('es').get('index')
        self.id_field = settings.get('id_field')
        self.bulk_size = settings.get('bulk_size', 1000)
        self.path_encoding = settings.get('path_encoding')

        self.actions = []

        log.debug('ESStorer instance created: %s', self.client)

    def get(self, doc_id, index):
        log.debug('Getting _id = %s from index %s', doc_id, index)

        try:
            return self.client.get(index=index, doc_type=self.doc_type,
                                   id=doc_id)
        except Exception as e:
            log.warn('Cannot get doc with ID = %s because: %s', doc_id, e)

    def search(self, **kwargs):
        q = kwargs.get('q', '*')
        sort = kwargs.get('sort', 'timestamp')
        search_after = kwargs.get('search_after')
        size = kwargs.get('size', 50)
        source = kwargs.get('source')
        extra = dict(
                size=size)

        if search_after:
            extra.update(dict(search_after=search_after))

        s = Search(using=self.client, index=self.index_name)
        if source:
            s = s.source(source)
        s = s.sort(sort)
        s = s.query(Q('query_string', query=q))
        s = s.extra(**extra)

        log.info('Query: %s', s.to_dict())

        r = s.execute()
        count = r.hits.total
        took = r.took

        result = r, count, took

        return result

    def partial_update_from_query(
            self, index, query, transform, last_updated=True):

        gc.collect()
        err_ids = []

        def it():
            batch = []

            log.info('Received query: %s', query)

            s = Search(
                    using=self.client,
                    index=index,
                    doc_type=self.doc_type)
            s = s.update_from_dict(query)

            log.info('Running query: %s', s.to_dict())

            # this loop shold spin `bulk_size` times
            for doc in s.scan():
                batch.append(doc)

            log.info('Accumulated %d items', len(batch))

            for doc in batch:
                data = doc.to_dict()
                _id = doc.meta.id
                data['_id'] = _id

                log.debug('Working on doc %s', data)

                try:
                    try:
                        doc_body = transform(data)
                        log.debug('Invoking transform on ID = %s', _id)
                    except Exception as e:
                        log.warn(
                            'Error while transforming doc ID = %s: %s',
                            _id, e)
                        raise e

                    if doc_body:
                        if last_updated:
                            doc_body['last_updated'] = datetime.now()

                        op = self.partial_update_op(
                                doc_id=_id,
                                index=index,
                                doc_body=doc_body,
                                doc_type=self.doc_type)
                        yield op
                except Exception as e:
                    log.warn('Cannot process doc ID = %s: %s', _id, e)
                    err_ids.append(_id)
            del(batch)

        try:
            # call the iterator via bulk
            self.bulk(it())
            log.info('Invoking self.bulk(it())')
        except Exception as e:
            log.warn('Error in bulk on query = %s because: %s', query, e)

        return err_ids

    def bulk_index_from_it(
            self, index, it, transform=lambda x: x, last_updated=True):

        gc.collect()
        err_ids = []

        def _it():
            for doc_body in it:
                try:
                    log.debug('Working on record: %s', doc_body)
                    _id = doc_body.get(self.id_field)

                    try:
                        doc_body = transform(doc_body)
                    except Exception as e:
                        log.warn(
                                'Error while transforming doc ID = %s: %s',
                                _id, e)
                        raise e

                    if doc_body:
                        if last_updated:
                            doc_body['last_updated'] = datetime.now()

                        op = self.partial_index_op(
                                doc_id=_id,
                                index=index,
                                doc_body=doc_body,
                                doc_type=self.doc_type)
                        yield op
                except Exception as e:
                    log.warn('Cannot process doc ID = %s: %s', _id, e)
                    err_ids.append(_id)

        try:
            self.bulk(_it())
            log.info('Invoked self.bulk(_it())')
        except Exception as e:
            log.warn('Error in bulk index because: %s', e)

        return err_ids

    def create_op(
                self, doc_id, index, doc_body, op_type='update',
                doc_type=None):
        if not doc_id:
            raise Exception('Invalid document ID: %s', doc_id)

        if not doc_type:
            doc_type = self.doc_type

        # remove _id
        if '_id' in doc_body:
            del(doc_body['_id'])

        if op_type == 'update':
            body = {
                    'doc': doc_body
                    }
        else:
            body = doc_body

        op_template = {
            '_id': doc_id,
            '_op_type': op_type,
            '_retry_on_conflict': 3,
            '_index': index,
            '_type': doc_type,
            '_source': body
        }

        return op_template.copy()

    def partial_index_op(self, doc_id, index, doc_body, doc_type=None):
        return self.create_op(
                doc_id=doc_id,
                index=index,
                doc_body=doc_body,
                op_type='index',
                doc_type=doc_type)

    def partial_update_op(
            self, doc_id, index, doc_body, doc_type=None):
        return self.create_op(
                doc_id=doc_id,
                index=index,
                doc_body=doc_body,
                op_type='update',
                doc_type=doc_type)

    def index(self, doc_id, index, source):
        log.debug('Storing _id = %s <- %s', doc_id, source)
        try:
            self.client.index(id=doc_id, index=index, doc_type=self.doc_type,
                              body=source)
        except Exception as e:
            log.warn('Cannot index %s because: %s', doc_id, e)

    def bulk(self, it):
        try:
            log.info('Sending bulk request on iterable/generator')
            args = dict(client=self.client,
                        actions=it,
                        chunk_size=self.bulk_size,
                        raise_on_exception=False,
                        raise_on_error=False,
                        stats_only=False,
                        request_timeout=self.timeout)

            res_succ, res_err = helpers.bulk(**args)

            log.info(
                    'Sent bulk request on queue iterator: '
                    'successfull ops = %d, failed ops = %d',
                    res_succ, len(res_err))

            for res in res_err:
                log.warn('Error response: %s', res)
        except Exception as e:
            log.error('Error in storing: %s', e, exc_info=True)

    def get_fields(self, index):
        return self.client.indices.get_mapping(index, doc_type=self.doc_type)

    def count(self, index, query):
        try:
            s = Search(
                    using=self.client,
                    index=index,
                    doc_type=self.doc_type). \
                            update_from_dict(query)
            log.info('Querying: %s', s.to_dict())

            return s.count()
        except Exception as e:
            log.warn('Cannot count: %s', e)

    def scan(self, index, query, limit=None, id_only=False):
        size = self.bulk_size
        max_records = None
        cnt = 0

        if isinstance(limit, int):
            if limit > 0:
                size = min(limit, size)
                max_records = limit

        kw = dict(
            index=index,
            query=query,
            size=size
        )

        if id_only:
            kw['_source'] = ['_id']

        log.debug('Scanning for %s (size = %d, index = %s)',
                  query, size, index)

        for hit in helpers.scan(self.client, **kw):
            if max_records:
                if cnt >= max_records:
                    log.debug('Stopping after pulling %d records'
                              ' as requested', cnt)
                    raise StopIteration

            log.debug('Yielding %s', hit['_id'])
            cnt += 1

            if id_only:
                yield hit.get('_id')
            else:
                yield hit

    def paginate(self, index, q='*', limit=None, size=None, id_only=True):
        if not size:
            size = self.bulk_size

        log.info('Limit %s, size %s (q = "%s")', limit, size, q)

        s = Search(
                using=self.client,
                index=index,
                doc_type=self.doc_type)
        s = s.query(Q('query_string', query=q))

        if limit:
            size = min(size, limit)
            s = s.extra(size=size)

        s = s.params(
                scroll='20m',
                size=size)

        if id_only:
            s = s.source(False)

        log.debug('Query: %s', simplejson.dumps(s.to_dict(), indent=2))

        hits = []
        overall = 0

        for h in s.scan():
            if limit is not None and overall >= limit:
                raise StopIteration()

            log.debug('Hit: %s (progress: %d)', h.meta.id, overall)
            if overall < limit or not limit:
                if id_only:
                    hits.append(h.meta.id)
                else:
                    hits.append(h.to_dict())

                if len(hits) == size:
                    yield iter(hits)
                    hits = []
                    overall += size

        if len(hits):
            yield iter(hits)
        else:
            raise StopIteration()


ES = ESStorer
