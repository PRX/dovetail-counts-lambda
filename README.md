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

```json
{
  "le": "the-listener-episode",
  "digest": "BtTifRE9b9iscgXovKINxPG5HX4Iqzlu1851WvgcCPY",
  "start": 489274,
  "end": 21229635,
  "total": 21229636,
  "region": "us-west-2"
}
```

This exactly what the [dovetail-bytes-lambda](https://github.com/PRX/dovetail-bytes-lambda) logs,
and tells us which listener-episode and arrangement-digest to lookup. The kinesis
data also includes a milliseconds "timestamp" of when the CloudWatch log line was
logged.

### Dovetail Arrangements

To find the arrangement of files that made up this mp3, we look in the S3
bucket `s3://${S3_BUCKET}/${S3_PREFIX}/_arrangements/${DIGEST}.json` or in a
DynamoDB arrangements table configured by the `ARRANGEMENTS_DDB_TABLE` env.

This json was set by either the [dovetail-stitch-lambda](https://github.com/PRX/dovetail-stitch-lambda)
or the [dovetail-cdn-arranger](https://github.com/PRX/dovetail-cdn-arranger)
when it creates the stitched file, and has the format:

```
{
  "version": 4,
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
    "a": {"f": "mp3", "b": 128, "c": 2, "s": 44100},
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
lambda "pushes" each request onto a list of bytes for each listener-episode +
utc-day + arrangement-digest. A lua function in the Redis lib accomplishes this.

Since these are keyed on the UTC day the byte-downloads occurred on, we can
safely expire them slightly after midnight. When we're fairly sure no straggler
downloaded-bytes will be coming in over kinesis.

```
redis:6379> GET dtcounts:bytes:<listener-episode>/2019-02-28/<digest>
"0-1,250289-360548,489274-21229635"
```

## Outputs

After pushing the new byte-range to Redis, we also get back the _complete_ range
downloaded for that listener-episode-day-digest. That can be compared to the arrangement to
determine how many total-bytes, and bytes-of-each-segment were downloaded. After
a threshold seconds (or a percentage) of the entire file is downloaded, we then log
that as an IAB-2.0 complaint download. For segments, we wait for _all_ the bytes
to be downloaded before sending an IAB-2.0 complaint impression.

Since we might receive additional requests _after we've logged a download_, we
also lock the impression via a redis hash:

```
redis:6379> HGETALL dtcounts:imp:<listener-episode>:2019-02-28:<digest>
1) "0"
2) ""
3) "1"
4) ""
5) "2"
6) ""
7) "all"
8) ""
```

The TTL on this (`REDIS_IMPRESSION_TTL`) defaults to 24 hours. This should prevent
logging duplicate downloads/impressions until the `<utc-day>` rolls over to the
next day.

### Digest Cache

Right now, it's possible a listener will download _different arrangements_ of the
same episode throughout a single utc-day. In this case, we only want to count the
**first** arrangement-digest we get a complete download/impression for. This is
accomplished by locking a redis key to the arrangement-digest for that user, for
24-hours (or `REDIS_IMPRESSION_TTL`):

```
redis:6379> GET dtcounts:imp:<listener-episode>:2019-02-28
"<the-arrangement-digest>"
```

Downloads/impressions against other digests will be logged to kinesis, but they
will include the flags `{isDuplicate: true, cause: 'digestCache'}`.

**NOTE**: this is likely a temporary measure, if we start locking listeners to
a single arrangement for the entire 24-hour UTC day.

### Kinesis Impressions stream

The `KINESIS_IMPRESSION_STREAM` is the main output of this function. These should be processed by another lambda and streamed to BigQuery via the Dovetail [analytics-ingest-lambda](https://github.com/PRX/analytics-ingest-lambda) These kinesis json records have the format:

```json
{
  "type": "bytes",
  "timestamp": 1539206255516,
  "listenerEpisode": "some-listener-episode",
  "digest": "the-arrangement-digest",
  "bytes": 9999,
  "seconds": 1.84,
  "percent": 0.65,
  "isDuplicate": true,
  "cause": "digestCache"
}
```

or

```json
{
  "type": "segmentbytes",
  "timestamp": 1539206255516,
  "listenerEpisode": "some-listener-episode",
  "digest": "the-arrangement-digest",
  "segment": 3,
  "isDuplicate": true,
  "cause": "digestCache"
}
```

## Error handling

Generally, this Lambda attempts to log any errors, but only passes things that
can truly be retried back to the
the callback() function. Instead, it just allows the origin-pull request to
proceed, and let CloudFront return whatever it finds in S3.

# Installation

To get started, first install dev dependencies with `yarn`. Then run `yarn test`. End of list!

Or to use docker, just run `docker-compose build` and `docker-compose run test`.

# License

[AGPL License](https://www.gnu.org/licenses/agpl-3.0.html)
