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
  "uuid": "4ab8d1b8-38eb-49d0-aa27-c7074e529b73",
  "start": 489274,
  "end": 21229635,
  "total": 21229636,
  "digest": "BtTifRE9b9iscgXovKINxPG5HX4Iqzlu1851WvgcCPY\",
  "region": "us-west-2"
}
```

This exactly what the [dovetail-bytes-lambda](https://github.com/PRX/dovetail-bytes-lambda) logs,
and tells us which request-uuid and arrangement-digest to lookup.

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
lambda "pushes" each request onto a list of bytes for each request uuid. A lua
function in the Redis lib accomplishes this. These byte ranges also have a TTL
so they don't stick around forever.

```
redis:6379> dtcounts:bytes:4ab8d1b8-38eb-49d0-aa27-c7074e529b73
"0-1,250289-360548,489274-21229635"
```

## Outputs

After pushing the new byte-range to Redis, we also get back the _complete_ range
downloaded for that request-uuid.  That can be compared to the arrangement to
determine how many total-bytes, and bytes-of-each-segment were downloaded.  After
a threshold seconds (or a percentage) of the segment is downloaded, we then log
that as an IAB-2.0 complaint download/impression.

Since we might receive additional requests _after we've logged a download_, we
can end up firing a whole bunch of downloads for the same file/segment.  So it's
important that they're considered idempotent in BigQuery, or some sort of
unique constraint is applied later on.

### Kinesis missing arrangements stream

The `KINESIS_ARRANGEMENT_STREAM` ENV is (probably) temporary.  When configured, the lambda will push any non-v3 arrangement digests onto the stream.  Then some other processor can ping the dovetail-stitch-lambda, which does the actual work of calculating the segment-bytes of the arrangement and uploading the v3 json to S3.

### Kinesis Impressions stream

The `KINESIS_IMPRESSION_STREAM` is the main output of this function.  These should be processed by another lambda and streamed to BigQuery via the Dovetail [analytics-ingest-lambda](https://github.com/PRX/analytics-ingest-lambda)  These kinesis json records have the format:

```json
{
  "type": "bytes",
  "timestamp": 1539206255516,
  "request_uuid": "some-guid",
  "bytes_downloaded": 9999,
  "seconds_downloaded": 1.84,
  "percent_downloaded": 0.65
}
```

or

```json
{
  "type": "segmentbytes",
  "timestamp": 1539206255516,
  "request_uuid": "some-guid",
  "segment_index": 3,
  "bytes_downloaded": 9999,
  "seconds_downloaded": 1.84,
  "percent_downloaded": 0.65
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
