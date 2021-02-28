#!/usr/bin/env python3

import subprocess
import glob
import os

TEST_DIR = 'test_cases'
OUTPUT_DIR = 'test_cases/outputs'

class Colors(object):
    blue = '\033[95m'
    green = '\033[32m'
    red = '\033[91m'
    end = '\033[0m'
    bold = '\033[1m'
    underline = '\033[4m'

def main():
    # First call all unit tests
    for module in ['transaction_manager', 'transaction', 'lock_manager',
                   'sites']:
        print('Testing {}'.format(module))
        subprocess.call(['python3', '-m', 'v2.{}'.format(module)])

    # Then end-to-end tests
    passed = 0
    failed = []

    for t in glob.glob(TEST_DIR + '/*.txt'):
        read = OUTPUT_DIR + '/' + t[t.index('/')+1 :]
        if not os.path.exists(read):
            print('{}No output file {}{}'.format(Colors.blue, read,
                                                 Colors.end))
            continue

        try:
            out_py = subprocess.Popen(['python3', 'main.py', t, '--log-level', 'none'],
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)
            out_real = subprocess.Popen(['cat', read], stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)

            if out_py.stdout.read() == out_real.stdout.read():
                passed += 1
            else:
                failed.append(t)
        except Exception as e:
            print(e)
            print('Failed EXCEPTION', t)
            failed.append(t)

    for f in failed:
        print('{}{}Failed {}{}'.format(Colors.red, Colors.bold, f,
                                       Colors.end))
    print('{}{}'.format(Colors.green if len(failed) == 0 else Colors.red,
                        '-'*80))
    print('Passed {}/{} end to end tests'.format(
        passed, passed + len(failed)))
    print('{}{}'.format('-'*80, Colors.end))

if __name__ == '__main__':
    main()
