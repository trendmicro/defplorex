# DefPloreX (Public Release)
At [BlackHat USA 2017's Arsenal we've showcased
DefPloreX](https://www.blackhat.com/us-17/arsenal/schedule/index.html#defplorex-a-machine-learning-toolkit-for-large-scale-ecrime-forensics-8065),
an Elasticsearch-based toolkit that our team uses for large-scale processing,
analysis and visualization of e-crime records. In particular, we've
successfully been applying DefPloreX to the analysis of deface records (e.g., from web compromises);
 hence its name, Def(acement) eXPlorer (DefPloreX).

![DefPloreX Visualization](i/dpx-clusters-viz.png?raw=true "DefPloreX Visualization")

DefPloreX automatically organizes deface records by web pages' content and format (what we call ``template pages'').
This allows an analyst to easily investigate on campaigns, 
for example in discovering websites targeted by the same campaign or
attributing one or more actors to the same hacking group.
All of this without sacrificing the interactivity aspect of the investigation.

![Overview of DefPloreX](i/dpx-overall.png?raw=true "Overview of DefPloreX")

The full version of DefPloreX includes:

  * A thin wrapper to interact with an Elasticsearch backend (included in this release)
  * A distributed data-processing pipeline based on Celery (example included in this release)
  * An analysis component to extract information from deface web pages
  * A features extraction component to produce a compact, numerical and categorical representation of each web page
  * A statistical machine-learning component to automatically find groups of similar web pages

The input to DefPloreX is a feed of URLs describing the deface web pages,
including metadata such as the (declared) attacker name, timestamp, reason
for hacking that page, and so on. Separately, we also have a mirror of the
web pages at the time of compromise.

## Code Release
This repository contains the public release of DefPloreX. Technically speaking,
we're releasing an example use of the DefPloreX approach to distributed data
processing using Elasticsearch (ES). This is not meant to be a ready-to-use,
plug-n-play solution, but rather a framework that you can reuse, extend and
improve to adapt it to your needs.

The goal that guided us to implement DefPloreX was the need to efficiently 
analyze a large number of records (pages) for common aspects, recurrent attackers,
or groups of organized attackers. In other words, a typical e-crime
forensics task.

In this, the core challenge was to visit and analyze over 13 million web pages, 
parse their source code, analyze their resources (e.g.,
images, scripts), extract visual information, store the data so extracted in
a database, and query it to answer the typical questions that arise during
a post-mortem investigation. Given its popularity and scalability,
we've chosen Elasticsearch as our data storage solution. Since we wanted our
solution to be scalable, and given that visiting a web page (with an automated,
headless browser) takes at least 5 seconds, the only option was to distribute
the workload across several worker machines.

## Distributed Data Processing

Normally, to take full advantage of Elasticsearch's distributed
data-processing functionality, you need to resort to
[scripting](https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-scripting.html).
Although scripting is quite powerful and handy for small data-manipulation
tasks, it's a bit cumbersome to deploy and handle requires; and, in addition, it
requires full access to the Elasticsearch's client nodes. For example, if you
need to process all the documents in an Elastic index (e.g., to enrich them by
computing additional fields), you will have to choose one of the scripting
languages supported by Elastic, write a script, deploy it and run it. Needless
to say, your script will run within the context of the ES runtime,
with all the limitations that this implies. For example, should you need to use
Python, you're forced to use the Jython Java implementation of Python, which is
not the same as pure Python. For instance, some of the libraries that you may
want to use may not be supported, and so on. In other words, we don't want to depend
on the Elastic's scripting subsystem in our work :)

Instead, we take a more "detached" approach. We decouple the data-processing
part, making it independent from the Elasticsearch runtime and architecture,
and rely on ES exclusively as a data back-end to store, retrieve and
modify JSON documents. The coordination of the distributed computation is
delegated to a well-known and widely used distributed task queue:
[Celery](http://www.celeryproject.org/). The friendliness of Celery is
astonishing: from the programmer's perspective, all it requires is to write
your data-processing code by means of a function, and Celery will take care of
offloading the (expensive and long-running) computation to one of the available
workers.

![DefPloreX distributed data processing via Celery](i/dpx-celery.png?raw=true "DefPloreX distributed data processing via Celery")

For example, if you need to visit a web page with an automated headless browser,
all you need to do is to wrap your code into a function, let's say `visit_page`,
and decorate it with `@app.task` to inform Celery that this is a task:

```
@app.task
def visit_page(url):
    result = long_running_process(url)

    return result
```

Later on in your code, all you need to do is to call the function (almost) as
you would normally do:

```
visit_page.delay(url)
```

The `.delay()` function indicates that the function call will not execute
immediately, but instead will be "pushed" into a task list, from which an
available worker will pull it and do the work.

On the other end of the task list, you can launch as many workers as you need,
by simply keeping the Celery daemon active:

```
$ celery worker --autoscale=6,64
```

Assuming having a 64-core machine, this command spawns 6 concurrent processes, up
to 64 when more workload comes in. And of course you can add as many workers as
needed, from a single computer with a few tenths of cores, to a full rack
distributed across the globe. In our deployment, we have 5 machines, with
a total of 128 cores. With these modest resources, we were able to visit the
entire collection of over 13 million web pages in a week. Adding more cores would have
made the analysis even faster.

# Document Transformations
From this moment on, we have a solid foundation to efficiently transform JSON
documents stored in the Elastic index. Therefore, we "encode" any operation
that we need to perform in DefPloreX by means of a few lines of Python code. For
example, we often need to "tag" JSON documents to mark those that have been
processed. To this end, as exemplified in this repository, we use the
`TagTransformer` transformation. As any other transform, this function receives one JSON
document and returns the newly added fields, or the modified fields.

```
class TagTransformer(Transformer):
    """
    Example transform to append tag to a record.
    """
    _name = 'tag'                   # unique name

    def __call__(self, doc, *args, **kwargs):
        doc = super(TagTransformer, self).__call__(
                doc, *args, **kwargs)

        tag = kwargs.get('tag')     # tag that we want to apply to the JSON

        if not tag:
            log.debug('No tags supplied, skipping')
            return []

        tags = doc.get('tags', [])  # get the 'tags' field from the existing JSON doc

        if tags:
            log.debug('Found tags: %s', tags)

        tags.append(tag)            # append the new tag
        tags = list(set(tags))      # remove duplicates

        log.debug('Updated tags: %s', tags)

        return dict(tags=tags)      # return the enriched JSON
```

The output of this transformation is automatically handled by our Elasticsearch 
wrapper (see `backend.elastic.ESStorer`) and the
`transformer.Pipeline` class, which merges the new (partial) document with the
original one and saves it into the ES index. Actually, this is
performed in bulk: that is, every worker consumes and processes a given amount
of documents at each round (default is 1000). To summarize: given a query, we
enqueue all the IDs of the documents that match that query. The queue consumers
will pull 1000 IDs at a time, query Elastic for the respective documents,
transform them, and push them back on Elastic as update operations.

Other transformations that we have implemented (briefly explained in
the following) include for example visiting the web pages with an automated,
headless browser, extracting information from the visited web pages,
calculating numerical features, and so on. Every task is expressed by means of
a subclass of `Transformer`, which takes as input a document, and returns the
enriched or modified fields.

## Extracted Information
From each web page, we were interested in collecting two "sides" of the same
story: a "static" view of the page (e.g., non-interpreted resources, scripts,
text) and a "dynamic" view of the same page (e.g., rendered page with DOM
modifications and so on). In concrete, the full version of DefPloreX can
extract URLs, e-mail addresses, social-network nicknames and handles, hashtags,
images, file metadata, summarized text, and so on. These information captures the
main characteristics of a defaced web page.

![Extracted data from each web page](i/dpx-extraction.png?raw=true "Extracted data from each page")

## Scalable Data Clustering
We approach the problem of finding groups of related deface pages
(e.g., hacktivism campaigns) as a typical data-mining problem. We assume that
there are recurring and similar characteristics among these pages that we can
capture and use as clustering features. For example, we assume that the same
attacker will reuse the same web snippets or templates (albeit with minimal variations)
within the same campaign. We capture this and other aspects by extracting
numerical and categorical features from the data that we obtained by analyzing
each page (static and dynamic view). To this end, we express the following
task by means of a transform function.

For example, here's an excerpt of the features that we compute from
each of our documents:

```
{
  "n_urls": 135,
  "n_object": 0,
  "n_embed": 0,
  "n_telephone": 8,
  "n_email": 1,
  "n_img": 18,
  "n_link": 0,
  "n_sound_urls": 0,
  "n_anchor": 60,
  "n_meta": 4,
  "n_resource": 0,
  "n_iframe": 0,
  "n_script": 34,
  "n_hashtag": 0,
  "n_style": 9,
  "n_twitter": 1,
  "avg_color": "#000000",
  "frac_letters_in_title": 0.6979166666666666,
  "frac_punct_in_title": 0.17708333333333334,
  "frac_whitespace_in_title": 0.0625,
  "frac_digits_in_title": 0.0625
}
```

![Feature extraction](i/dpx-features.png?raw=true "Feature extraction")

At this point we could use any clustering algorithm to find groups. However,
this would not be the most efficient solution, at least in general, because
we would need to compare all pairs of our collection of 13 million records, 
calculate "some" form of distance (e.g., ssdeep), and then start forming groups by
means of such distance.

We take a different approach, which is approximate but way faster. As a result,
we're able to cluster our entire collection of 13 million documents in less than a
minute, and we dynamically configure the clustering features on demand (i.e., at
each clustering execution).

Intuitively, we would like to be able to find logical groups of web pages that
share "similar" feature values. Instead of approaching this problem as
a distance-metric calculation task, we use the concept of "feature binning" or
"feature quantization". In simple words, we want all the web pages with a "low
number of URLs" to fall in the same cluster. At the same time, we want all the
web pages with a "high number of URLs" to fall in another cluster. And so on,
for all the features. In other words, the clustering task becomes a "group-by"
task, which is natively and well supported by all database engines. In the case of
Elastic, it's efficiently implemented in a map-reduce fashion, effectively distributing
the workload across all the available nodes.

The missing piece is how we obtain these "low, medium, high" values from the
original, numerical feature values. For instance, is "42 URLs" considered low,
high, or medium? To this end, we look at the statistical distribution of each feature,
and divide its space into intervals according to estimated percentiles. For instance,
the values below the 25% percentile are considered low, those between 25-50% percentile
are medium, and those between 50% and 75% are high. Those above the 75% percentile
are outliers. This is just an example, of course.

![Feature quantization and clustering](i/dpx-binning.png?raw=true "Feature quantization and clustering")

It turns out that Elasticsearch already supports the calculation of a few
statistical metrics, among which we happily found the percentiles. So all we need
to do is asking Elastic to compute the percentiles of each feature -- done in a matter
of few seconds. Then, we store these percentiles
and use them as thresholds to quantize the numerical features.

For example, here's an excerpt of four equally-spaced percentiles (from 1%
to 99%) that we obtaine from our collection:

```
"features": {
	"n_style": [
	  0,
	  2,
	  5,
	  10
	],
	"n_anchor": [
	  0,
	  10,
	  34,
	  284.78097304328793
	],
	"n_urls": [
	  0,
	  6.999999999999999,
	  19.575392097264313,
	  201.65553368092415
	],
	"n_hashtag": [
	  0,
	  2.2336270350272462,
	  5,
	  16
	],
	"n_script": [
	  0,
	  4,
	  12,
	  45
	],
	"n_sound_urls": [
	  0,
	  1,
	  2.4871283217280453,
	  7
	],
...
}
```

Overall, for each page, we obtain a vector as the following that we store in ES.

```
{
  "n_urls": H,
  "n_object": L,
  "n_embed": L,
  "n_telephone": M,
  "n_email": L,
  "n_img": M,
  "n_link": L,
  "n_sound_urls": L,
  "n_anchor": M,
  "n_meta": L,
  "n_resource": L,
  "n_iframe": L,
  "n_script": M,
  "n_hashtag": L,
  "n_style": L,
  "n_twitter": L,
  "avg_color": "#000000",
  "frac_letters_in_title": M,
  "frac_punct_in_title": L,
  "frac_whitespace_in_title": L,
  "frac_digits_in_title": L
}
```

At this point, the web operator (the analyst) simply chooses the features for data pivoting, and
runs an Elasticsearch aggregate query, which is natively supported.

In the remainder of this page you can see some example results.

![Feature quantization and clustering (visualized)](i/dpx-binning-viz.png?raw=true "Feature quantization and clustering (visualized)")

![Feature quantization and clustering (visualized)](i/dpx-binned-records-viz.png?raw=true "Feature quantization and clustering (visualized)")

# License
```
Copyright (c) 2017, Trend Micro Incorporated
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

The views and conclusions contained in the software and documentation are
those of the authors and should not be interpreted as representing official
policies, either expressed or implied, of the FreeBSD Project.
```
