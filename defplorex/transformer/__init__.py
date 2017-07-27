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

from defplorex.transformer.tag import TagTransformer

log = logging.getLogger(__name__)

__all__ = [
    'TagTransformer'
]

classes = [
    TagTransformer
]


class TransformerFactory(object):
    registry = {t._name: t for t in classes}

    @classmethod
    def get_by_name(cls, name):
        return cls.registry.get(name)

    @classmethod
    def get_by_list(cls, name_lst):
        return [cls.get_by_name(name) for name in name_lst]

    @classmethod
    def get_names(cls):
        return cls.registry.keys()

    @classmethod
    def get_classes(cls):
        return cls.registry.items()


class Pipeline(object):
    @staticmethod
    def chain(doc, transformers, updates_only=True, *args, **kwargs):
        doc = doc.copy()

        if '_source' in doc:
            doc = doc.get('_source', {})

        kwargs.update(**dict(original_doc=doc))
        updates = {}

        for transformer in transformers:
            _ = transformer(updates.copy(), *args, **kwargs)
            updates.update(**_)

        if updates_only:
            return updates

        doc.update(**updates)

        return doc
