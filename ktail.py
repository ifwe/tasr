#!/usr/bin/python

import sys
import os
import logging
import argparse
from tasr.ktail import KTail


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('topic', help='The Kafka topic to tail. (required)')
    parser.add_argument('-v', '--verbose', action='count')
    parser.add_argument('--tasr', default='http://tasr.tagged.com')
    parser.add_argument('--kafka', nargs='*', default=['kafkadatahub01:9092'])
    args = parser.parse_args(argv[1:])

    verbosity = logging.ERROR
    if args.verbose > 2:
        verbosity = logging.DEBUG
    elif args.verbose == 2:
        verbosity = logging.INFO
    elif args.verbose == 1:
        verbosity = logging.WARN

    logging.getLogger().setLevel(verbosity)
    logging.info('topic: %s, tasr_url: %s, bootstrap_servers: %s',
                 args.topic, args.tasr, args.kafka)

    try:
        kt = KTail(args.topic, bootstrap_servers=args.kafka, tasr_url=args.tasr)
        kt.log.setLevel(verbosity)
        for event_dict in kt:
            sys.stdout.write(str(event_dict) + '\n')
    except KeyboardInterrupt:
        sys.stderr.write('Interrupted.\n')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

if __name__ == "__main__":
    main(sys.argv)

