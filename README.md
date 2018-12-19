# Dovetail Counts Lambda

Count which bytes of a multi-segment mp3 were downloaded

# Description

This lambda function processes incoming kinesis events, counts how many bytes
were downloaded by each request, and when a threshold number of seconds has
been sent, logs that request-uuid as a "download", in compliance with the
[IAB Podcast Measurement Technical Guidelines v2.0](https://www.iab.com/wp-content/uploads/2017/12/Podcast_Measurement_v2-Final-Dec2017.pdf)

## Inputs

### Kinesis events

Excluding a bunch of gzip-ing and base64-ing done by kinesis as it moves CloudWatch
logs across regions, each incoming "event" looks something like:

```
{
  "ls": "the-listener-session",
  "digest": "BtTifRE9b9iscgXovKINxPG5HX4Iqzlu1851WvgcCPY",
  "start": 489274,
  "end": 21229635,
  "total": 21229636,
  "region": "us-west-2"
}
```

This exactly what the [dovetail-bytes-lambda](https://github.com/PRX/dovetail-bytes-lambda) logs,
and tells us which listener-session and arrangement-digest to lookup.

### Dovetail Arrangements

To find the arrangement of files that made up this mp3, we look in the S3
bucket `s3://${S3_BUCKET}/${S3_PREFIX}/_arrangements/${DIGEST}.json`. This file
is created by dovetail when it creates the stitched file, and has the format:

```
{
  "version": 3,
  "data": {
    "f": [
      "https://f.prxu.org/70/d87b79c6-734b-4022-b4ac-4c7da706a505/31f4ddc8-7f66-42a7-91aa-33a2202ce94f.mp3",
      "http://static.adzerk.net/Advertisers/68649ed71ce74259a57f24ce13e5a6cc.mp3",
      "http://static.adzerk.net/Advertisers/68649ed71ce74259a57f24ce13e5a6cc.mp3",
      "http://static.adzerk.net/Advertisers/b09ccdfb797d45a98fc0d0caca13a0b8.mp3",
      "https://f.prxu.org/70/d87b79c6-734b-4022-b4ac-4c7da706a505/91df9d5a-dd0a-471e-89b2-742203bd95c9.mp3",
      "https://f.prxu.org/70/d87b79c6-734b-4022-b4ac-4c7da706a505/111ada0d-46bc-440f-9e06-7cb07a4de83c.mp3",
      "http://static.adzerk.net/Advertisers/b4c8dde093294f388b59e94540876345.mp3"
    ],
    "t": "oaaaooi",
    "b": [
      81204,
      165841,
      166625,
      167409,
      196405,
      8610446,
      8682283,
      8745238
    ]
  }
}
```

### Redis Byte-Ranges

Since there is no guarantee when we'll get each byte-range request event, this
lambda "pushes" each request onto a list of bytes for each listener-session +
arrangement-digest. A lua function in the Redis lib accomplishes this. These
byte ranges also have a TTL so they don't stick around forever.

```
redis:6379> GET dtcounts:bytes:<listener-session>/<digest>
"0-1,250289-360548,489274-21229635"
```

## Outputs

After pushing the new byte-range to Redis, we also get back the _complete_ range
downloaded for that listener-session-digest.  That can be compared to the arrangement to
determine how many total-bytes, and bytes-of-each-segment were downloaded.  After
a threshold seconds (or a percentage) of the entire file is downloaded, we then log
that as an IAB-2.0 complaint download.  For segments, we wait for _all_ the bytes
to be downloaded before sending an IAB-2.0 complaint impression.

Since we might receive additional requests _after we've logged a download_, we
also lock the impression via a redis hash:

```
redis:6379> HGETALL dtcounts:imp:<listener-session>/<digest>
1) "0"
2) ""
3) "1"
4) ""
5) "2"
6) ""
7) "all"
8) ""
```

The TTL on this (`REDIS_IMPRESSION_TTL`) defaults to 1 hour. So this lock cannot
permanently prevent duplicate logged downloads/impressions. It just cuts down
on the most common dups, since most clients will download an entire file and
then never use that CDN url again.

### Kinesis missing arrangements stream

The `KINESIS_ARRANGEMENT_STREAM` ENV is (probably) temporary.  When configured, the lambda will push any non-v3 arrangement digests onto the stream.  Then some other processor can ping the dovetail-stitch-lambda, which does the actual work of calculating the segment-bytes of the arrangement and uploading the v3 json to S3.

### Kinesis Impressions stream

The `KINESIS_IMPRESSION_STREAM` is the main output of this function.  These should be processed by another lambda and streamed to BigQuery via the Dovetail [analytics-ingest-lambda](https://github.com/PRX/analytics-ingest-lambda)  These kinesis json records have the format:

```json
{
  "type": "bytes",
  "timestamp": 1539206255516,
  "listenerSession": "some-listener-session",
  "digest": "the-arrangement-digest",
  "bytes": 9999,
  "seconds": 1.84,
  "percent": 0.65
}
```

or

```json
{
  "type": "segmentbytes",
  "timestamp": 1539206255516,
  "listenerSession": "some-listener-session",
  "digest": "the-arrangement-digest",
  "segment": 3
}
```

## Error handling

Generally, this Lambda attempts to log any errors, but only passes things that
can truly be retried back to the
the callback() function.  Instead, it just allows the origin-pull request to
proceed, and let CloudFront return whatever it finds in S3.

# Installation

To get started, first install dev dependencies with `yarn`.  Then run `yarn test`.  End of list!

Or to use docker, just run `docker-compose build` and `docker-compose run test`.

# License

[AGPL License](https://www.gnu.org/licenses/agpl-3.0.html)
