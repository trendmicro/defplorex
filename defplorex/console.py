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

import sys
import time
import logging

# 3rd partymodules
import click
import simplejson

from elasticsearch_dsl import Search, Q

# local modules
from defplorex.loggers import config_logger
from defplorex.config import load_settings
from defplorex.backend.elastic import ES
from defplorex.transformer import TransformerFactory
from defplorex.utils import (
        SlowOverallFancyBar,
        SlowFancyBar,
        fopen)

# locals
log = logging.getLogger(__name__)
settings = load_settings()
es = ES(settings)
TR = TransformerFactory.get_names()

INDEX = settings.get('es').get('index')


@click.group()
@click.option(
        '--debug', '-d',
        is_flag=True, default=False, help='Enable debugging output')
def cli(debug):
    config_logger(debug=debug)

    # NOTE put any preparatory task here

    log.info('Command line ready...')


@cli.command()
def show_settings():
    """Print the configuration settings"""
    simplejson.dump(settings, sys.stdout)


@cli.group()
def process():
    """Distributed data-processing commands"""
    pass


@cli.group()
def elastic():
    """ES commands"""
    log.info('Elasticsearch commands')


@process.command()
@click.option(
        '--index', '-i',
        help='Read from index',
        metavar='F', default=INDEX)
@click.option(
        '--transformer', '-T',
        multiple=True,
        type=click.Choice(TR),
        metavar='T',
        help='Transformation: {}'.format(TR))
@click.option(
        '--limit', '-l', type=int,
        metavar='L', help='Limit number of records')
@click.option(
        '--tag', '-t',
        metavar='TAG', help='Tag the records')
@click.option(
        '--reindex', '-r',
        is_flag=True, default=False, help='Do not update,'
        ' but re-index (expensive)')
@click.option(
        '--now', '-n',
        is_flag=True, default=False, help='Execute locally')
@click.option(
        '--ephemeral', '-e',
        is_flag=True, default=False, help='Dry run')
@click.argument('q', metavar='<q>')
def enqueue(index, transformer, limit, tag, reindex, now, ephemeral, q):
    """
    Read from index according to query, process, and write to index
    """
    if not transformer:
        log.warn('Please choose at least one transform among %s', TR)

    from defplorex.tasks import processor_task

    log.info('Working on index %s', index)

    kwargs = dict(
            update=not reindex,
            ephemeral=ephemeral)

    if tag:
        kwargs.update(**dict(tag=tag))

    # iterator that paginates through records
    it = es.paginate(
            index=index,
            q=q,
            limit=limit,
            id_only=True)

    # enqueue one task per page of records
    for ids in it:
        ids = list(ids)
        click.echo('Launching task with {} IDs'.format(len(ids)))

        kwargs.update(**dict(transformers_lst=transformer))

        s = processor_task.s(ids, index, **kwargs)

        if now:
            res = s()
        else:
            res = s.delay()
            if ephemeral:
                res = res.get()

        if ephemeral:
            click.echo(simplejson.dumps(res, indent=2))


@process.command()
@click.option('--index', '-i', default=INDEX, help='Read from index')
@click.option('--delta', '-D', help='Measure delta from beginning',
              is_flag=True)
@click.argument('query_string', metavar='<query_string>')
def monitor(index, delta, query_string):
    click.clear()

    def cnt():
        q = Q('query_string', query=query_string)
        s = Search(
                using=es.client,
                index=index).query(q)
        return s.count()

    N = cnt()
    tot = Search(using=es.client, index=index).count()

    if not delta:
        N = tot

    log.info('Processing %d records (total: %d)', N, tot)

    click.echo('You can exit by CTRL-C: results will still process')

    bar = SlowOverallFancyBar('', max=N, grand_total=tot)
    while True:
        time.sleep(5.0)
        try:
            n = cnt()
            if isinstance(n, int):
                if delta:
                    done = N - n
                else:
                    done = n
                bar.goto(done)
        except Exception as e:
            log.warn('Cannot count: %s', e)
    bar.finish()


@elastic.command()
@click.argument('index')
@click.argument('mappings_and_settings', type=click.File('rb'))
def create_index(index, mappings_and_settings):
    """Create an index given mappings and settings as a JSON"""
    body = simplejson.load(mappings_and_settings)

    click.confirm('Create index "%s"?' % index, abort=True)

    es.client.indices.create(index=index, body=body)

    log.info('Index created')


@elastic.command()
@click.argument('index')
def delete_index(index):
    """Delete an index"""
    click.clear()

    click.confirm(
            click.style(
                'Really DELETE index "%s"?' % index,
                fg='white',
                bg='red'), abort=True)

    es.client.indices.delete(index=index)

    log.info('Index deleted')


@elastic.command()
@click.option(
        '--use-helper',
        '-H',
        is_flag=True,
        default=False,
        help='Use old helper API')
@click.argument('from_index')
@click.argument('to_index')
def clone_index(use_helper, from_index, to_index):
    """Clone an index"""
    from elasticsearch_dsl import Search
    from elasticsearch.helpers import reindex

    click.clear()

    if not es.client.indices.exists(index=to_index):
        click.secho('%s not existing!'.format(to_index), fg='red')
        return 1

    cnt = Search(using=es.client, index=to_index).count()
    message = 'Index %s already exists (%d records). Overwrite?' % (
            to_index, cnt)

    click.confirm(message, abort=True)

    if use_helper:
        reindex(
                client=es.client,
                source_index=from_index,
                target_index=to_index)
    else:
        es.client.reindex(
                body=dict(
                    source=dict(index=from_index),
                    dest=dict(index=to_index)),
                wait_for_completion=False)


@elastic.command()
@click.argument('from_index')
@click.argument('to_index')
def monitor_clone_index(from_index, to_index):
    """Monitor the size of an index"""
    from elasticsearch_dsl import Search

    click.clear()

    cnt = Search(using=es.client, index=from_index).count()

    bar = SlowFancyBar('', max=cnt)
    while True:
        time.sleep(2.0)
        _cnt = Search(using=es.client, index=to_index).count()
        bar.goto(_cnt)
    bar.finish()
