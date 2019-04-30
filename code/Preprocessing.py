#!/bin/python3

import fnmatch
import os
import sys
import token
import tokenize


PATTERN = '*.py'

# Directory to extract repository with python files only
DIR = '../preprocessed'


def preprocess(root_dir):
    """
    Function to extract python files from repositories and place them in preprocessed dir.
    It also eliminates comments.
    :param root_dir:
    :type root_dir: str
    """

    for root, dirs, files in os.walk(root_dir):
        for file in fnmatch.filter(files, PATTERN):
            print("Preprocessing {}.".format(root))

            with open(os.path.join(root, file), "r") as python_file:
                sub_dir = root.split('/')[2]
                reformated_root = root.lstrip('..').replace(
                    '/', '_') + '_stripped_' + file
                os.makedirs('{0}/{1}'.format(DIR, sub_dir), exist_ok=True)

                # New file to write python file
                output = open('{0}/{1}/{2}'.format(DIR,
                                                   sub_dir, reformated_root), 'w+')

                old_prev = None
                old_old_prev = None
                prev_tok_type = token.INDENT

                # Read file and split into tokens.
                tok_gen = tokenize.generate_tokens(python_file.readline)

                for tok_type, ttext, (slineno, scol), (elineno, ecol), ltext in tok_gen:

                    # Docstring
                    if tok_type == token.STRING and prev_tok_type == token.INDENT:
                        pass

                    # Comment
                    elif tok_type == tokenize.COMMENT:
                        pass

                    # Some file contains encoding information before comment.
                    elif tok_type == tokenize.STRING and prev_tok_type == tokenize.NL \
                            and old_prev == tokenize.NL and old_old_prev == tokenize.COMMENT:
                        pass
                    else:
                        output.write(ttext + ' ')

                    old_old_prev = old_prev
                    old_prev = prev_tok_type
                    prev_tok_type = tok_type


if __name__ == '__main__':

    # Check to see if correct argument number passed
    if len(sys.argv) < 2:
        print("Invalid directory specified!")
        print("Usage: python3 Metrics.py DIR_NAME")
        sys.exit(1)

    arg = sys.argv[1]
    preprocess(arg)
