#!/usr/bin/env python3
"""
Authors Conrad Christensen

Classes:
    main: loops over every command in input file and calls relevant function
          in TransactionManager
    Parser: Class that matches regular expressions to file input to determine
            what command is input'd and what arguments are provided
"""

import re
import sys
import enum
import logging
import argparse
from collections import namedtuple

from v2.transaction_manager import TransactionManager

# Logger handling done in global scope to make logger available. Set up is done
# here but other classes can simply grab the logger with the following line.
logger = logging.getLogger('txn_manager')
LOG_HANDLER = logging.StreamHandler()
LOG_HANDLER.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
logger.addHandler(LOG_HANDLER)
LOG_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'none': logging.NOTSET,
}

# Command object defined along with the types of commands
Command = namedtuple('Command', ['type', 'args'])

class CommandType(enum.Enum):
    begin = 0
    beginRO = 1
    read = 2
    write = 3
    dump_all = 4
    dump_site = 5
    dump_variable = 6
    end = 7
    fail = 8
    recover = 9


class Parser(object):
    # No front anchors needed because we use re.match, and back anchor is only
    # required if no comment start is found
    patterns = {
        CommandType.begin: re.compile('begin\(t([0-9]+)\)(//|$)'),
        CommandType.beginRO: re.compile('beginro\(t([0-9]+)\)(//|$)'),
        CommandType.read: re.compile('r\(t([0-9]+),x([0-9]+)\)(//|$)'),
        CommandType.write: re.compile('w\(t([0-9]+),x([0-9]+),([0-9]+)\)(//|$)'),
        CommandType.dump_all: re.compile('dump\(\)(//|$)'),
        CommandType.dump_site: re.compile('dump\(([0-9]+)\)(//|$)'),
        CommandType.dump_variable: re.compile('dump\(x([0-9]+)\)(//|$)'),
        CommandType.end: re.compile('end\(t([0-9]+)\)(//|$)'),
        CommandType.fail: re.compile('fail\(([0-9]+)\)(//|$)'),
        CommandType.recover: re.compile('recover\(([0-9]+)\)(//|$)'),
        None: re.compile('\s*(//|$)')
    }
    def __init__(self, file_handle):
        self._started_iter = False
        self._iter = iter(file_handle)
        logger.info("Create Parser object")

    def __iter__(self):
        """
            Create iterator of the parser and returns. Note that the iterator
            is just the parser itself (but this should be called to ensure
            it only gets called once.
        """
        if self._started_iter:
            raise ValueError('Cannot iterate twice')
        self._started_iter = True
        return self

    def __next__(self):
        """
            This function pulls the next line of data from the file and returns
            a Command object denoting the type and arguments (directly from the
            regex match). If the line was a comment then a None type returned
            with the argument replaced by the actual line read.
        """
        assert self._started_iter, 'Must start iterator to call next'

        oline = next(self._iter)
        line = oline.lower().replace(' ', '').replace('\t', '')

        for type_, pat in Parser.patterns.items():
            match = pat.match(line)
            if match:
                # Note the groups()[:-1] is to drop the (#|$) group at the end
                # which captures a comment or end-of-line for each match
                return Command(
                    type_, tuple(map(int, match.groups()[:-1]))
                    if type_ else oline)

        raise ValueError('No matches for line: {}'.format(oline))

def do_cmd(tm, cmd):
    if cmd.type == CommandType.begin:
        logger.debug('Command {} for txn T{}'.format(cmd.type, cmd.args[0]))
        tm.new_txn(cmd.args[0])
    elif cmd.type == CommandType.beginRO:
        logger.debug('Command {} for txn T{}'.format(cmd.type, cmd.args[0]))
        tm.new_txn(cmd.args[0], read_only=True)
    elif cmd.type == CommandType.read:
        logger.debug('Command {} for txn T{} on var x{}'.format(
            cmd.type, *cmd.args))
        tm.read(cmd.args[0], cmd.args[1])
    elif cmd.type == CommandType.write:
        logger.debug('Command {} for txn T{} on var x{} and value {}'.format(
            cmd.type, *cmd.args))
        tm.write(cmd.args[0], cmd.args[1], cmd.args[2])
    elif cmd.type == CommandType.dump_all:
        logger.debug('Command {}'.format(cmd.type))
        tm.dump()
    elif cmd.type == CommandType.dump_site:
        logger.debug('Command {} for site {}'.format(cmd.type, *cmd.args))
        tm.dump(site=cmd.args[0])
    elif cmd.type == CommandType.dump_variable:
        logger.debug('Command {} for variable x{}'.format(cmd.type, *cmd.args))
        tm.dump(var=cmd.args[0])
    elif cmd.type == CommandType.end:
        logger.debug('Command {} for txn T{}'.format(cmd.type, cmd.args[0]))
        tm.finish_txn(cmd.args[0])
    elif cmd.type == CommandType.fail:
        logger.debug('Command {} for site {}'.format(cmd.type, *cmd.args))
        tm.fail(cmd.args[0])
    elif cmd.type == CommandType.recover:
        logger.debug('Command {} for site {}'.format(cmd.type, *cmd.args))
        tm.recover(cmd.args[0])

def main(args):
    logger.setLevel(LOG_LEVELS[args.log_level])

    trans_man = TransactionManager(full_output=not args.min_output,
                                   log_writes=not args.no_write_log,
                                   test15_opt=not args.no_rec_site_opt)
    with (open(args.input_file, 'r') if args.input_file else sys.stdin) as fp:
        parseit = iter(Parser(fp))
        while True:
            try:
                cmd = next(parseit)
                if cmd.type is None:
                    # Remove trailing new line
                    logger.info('Blank or comment line: {}'.format(cmd.args[:-1]))
                else:
                    do_cmd(trans_man, cmd)
            except ValueError as e:
                raise
                logger.error(str(e)[:-1])
                logger.error('Continueing...')
            except StopIteration:
                logger.info('Done with file')
                break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run distributed database on test file')
    parser.add_argument('input_file', metavar='IN_FILE', type=str,
                        default=None, nargs='?', help='name of input file')
    parser.add_argument('--min-output', action='store_true',
                        help='produce only minimum output specified in doc')
    parser.add_argument('--no-write-log', action='store_true',
                        help='whether to stop logging of writes '
                        '(only applicable with full output)')
    parser.add_argument('--no-rec-site-opt', action='store_true',
                        help='turn off optimization that prevents deadlock in'
                        'the case of test case 15 (writing to recovered site)')
    parser.add_argument('--log-level', metavar='LEVEL', type=str,
                        choices=['debug', 'info', 'none'], default='none',
                        help='logging level')
    main(parser.parse_args())
