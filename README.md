# Audio Stream Proof of Concept

## Origin
This repo is a fork of the [AWS samples repo](https://github.com/aws-samples/amazon-kinesis-video-streams-consumer-library-for-python).

The aim was to attempt to be able to provide an audio feed out of AWS Connect for consumption by a summarising LLM.


## Features

### [Processor](./amazon_kinesis_video_consumer_library/kinesis_video_fragment_processor.py)

The processor leverages EBMLite to parse the KVS stream. KVS stream consists of a bytestream which itself defines several back to back MKV files which themselves contain our audio in two separate channels. EBML is the language used to define MKV files and it meant to be read in its byte format ([read more about EBML](https://datatracker.ietf.org/doc/rfc8794/)).

This processor provides tools to handle the MKV files once they've been pulled form the bytestream.


### [Parser](./amazon_kinesis_video_consumer_library/kinesis_video_streams_parser.py)

The parser is based on the Python Thread class - enabling multithreading. Multithreading is handy but not necessary as multithreading allows shared memory space whilst freeing up the [GIL](https://wiki.python.org/moin/GlobalInterpreterLock) when waiting on heavy IO bound operations such as fetching the next piece of the bytesream from MKV. The parser then uses callbacks to feed back the MKV fragments.

### [Consumer - protocol](./amazon_kinesis_video_consumer_library/kinesis_video_stream_consumer.py)

The module defines a [protocol](https://typing.python.org/en/latest/spec/protocol.html) which specifies what callbacks are need in order to integrate with the parser class

### [Slice Consumer](./audio_slice_consumer.py)

Defines one such implementation of the Consumer protocol. This handles dispatching by writing to disk though this function is intended to be modified for dispatching to an SQS Queue and an S3 Bucket.

### [example file](./kvs_consumer_library_example.py)

This is the file to run when you want to listen in on a call as things currently stand in the PoC. Simply run a command like:

```sh
uv run kvs_consumer_library_example.py name-of-your-kvs-audio-stream
```

# Running this for yourself

## env set up
Firstly, you need your local Python set up.

Ensure you have UV installed ([brew](https://brew.sh/) is a good choice of installer).  
```sh
brew install uv
```

Next setup Python:  
```sh
uv sync
```

## making the call

Everything is set up in the UAT environment. Call flow is `audio-stream-test` - this will trigger a Lambda `event-printer` which will print out the entire Lambda event, including the KVS stream ARN and the start fragment. You will need both of these pieces of information.  
example arn: `arn:aws:kinesisvideo:eu-west-2:759942772963:stream/rap-uat-voicemail-connect-rap-uat-connect-instance-ccaas-001-contact-fde1c9d7-df30-4b95-9833-22569c81aa02/1660714770128`  
example start fragment number: `91343852333181675028929762615222463140535793063`
Now you can call:
```sh
uv run kvs_consumer_library_example.py rap-uat-voicemail-connect-rap-uat-connect-instance-ccaas-001-contact-fde1c9d7-df30-4b95-9833-22569c81aa02 91343852333181675028929762615222463140535793063
```

If you want to talk to someone, ensure they are on the routing profile `dave rp` and are available.

Now all conversational snippets should make their way to the `dispatches/` directory as two separate audio streams.


# How does it work?

Audio is pulled off the KVS Stream live, each fragment is added to a buffer. Once the buffer fills to configured mx size the buffer is sliced somewhere between the configured min and max chunk size and that chunk is sent to dispatch.

# Sample

Sample audio from a live rip can be [found here](./the-dave-helpline/)