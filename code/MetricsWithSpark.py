#!/bin/python3

import json

from itertools import zip_longest
from pyspark import SparkContext


ROOT = '/home/datalitics_concepts/turing'
CSV_SOURCE = 'gs://dataproc-ff11f78a-01f1-451f-96ff-6fc82b35c314-europe-west2/url_list.csv'

REPO_ROOT_GS = 'gs://dataproc-ff11f78a-01f1-451f-96ff-6fc82b35c314-europe-west2/preprocessed'

# HDFS_ROOT = 'hdfs://turing/user/shina/preprocessed'

# A Directory in the Master
REPO_ROOT = '{0}/{1}'.format(ROOT, 'preprocessed')


def extract_package(line):
    """
    Function to extract package from line of code.
    :param line: Current line of the source code.
    :type line: str
    :return: Package extracted from line of code.

    >>> s0 = 'import bs4.BeautifulSoup'
    >>> extract_package(s0)
    ['bs4']
    >>> s1 = 'import snake, ladder, etc'
    >>> extract_package(s1)
    ['snake', 'ladder', 'etc']
    >>> s3 = 'from operator import add'
    >>> extract_package(s3)
    ['operator']
    """
    if '.' in line:
        return [line.split()[1].split('.')[0]]
    elif 'from' in line:
        return [line.split()[1]]
    elif ',' in line:
        libraries = line.split()[1:]
        return [library.strip(',') for library in libraries]
    else:
        return [line.split()[1]]


def extract_parameter(line):
    """
    Function to extract the parameters in a function.
    :param line: The line of code to be examined.
    :type line: str
    :return: list of parameters

    >>> def_line = 'def add(w, x, y, z):'
    >>> extract_parameter(def_line)
    ['w', 'x', 'y', 'z']
    """
    tmp = line.lstrip('def ').split(
        '(')[1].replace(')', '').replace(':', '').split(',')
    return list(
        filter('self'.__ne__, list(map(str, [parameter.strip() for parameter in tmp if not parameter.strip() == '']))))


def extract_variable(line):
    """
    Function extract variables in a line of code.
    :param line: The line of code to be examined.
    :type line: str
    :return: list of variables

    >>> var1 = 'a,b,c = [1,2,3]'
    >>> extract_variable(var1)
    ['a', 'b', 'c']
    >>> var2 = 'd,e,f = (5,6,7)'
    >>> extract_variable(var2)
    ['d', 'e', 'f']
    >>> var3 = 'gender = boy'
    >>> extract_variable(var3)
    ['gender']
    """

    if ',' in line and '=' in line:
        multiple_variable = line.split('=')[0].split(',')
        return list(map(str.strip, multiple_variable))

    tmp = line.split()
    variables = []
    for i in range(len(tmp) - 1):
        _token = tmp[i]
        if tmp[i + 1] == '=' and _token != '=':
            variables.append(_token)
    return variables


def grouper(iterable, padvalue=None):
    """
    Function to group a list to code into a tuple of 4 lines each.
    :param iterable: A List of source code lines.
    :type iterable: list
    :param padvalue: Value to be used for padding in case a list item is less than 4 tuples.
    :type padvalue: object
    :return: A list of tuples of lines of codes.

    Simply testing with numbers, it follows the same for strings.
    >>> l = [1,2,3,4,5,6,7,8]
    >>> grouper(l)
    [(1, 2, 3, 4), (5, 6, 7, 8)]
    """

    return list(zip_longest(*[iter(iterable)] * 4, fillvalue=padvalue))


def group_partition(partitions):
    """
    Function to
    :param partitions:
    :type partitions: list[list]
    :return: iterator of lists

    >>> l = [[1,2,3,4,5,6,7,8],[4,4,4,4,5,5,5,5]]
    >>> it = group_partition(l)
    >>> it.__next__()
    [(1, 2, 3, 4), (5, 6, 7, 8)]
    >>> it.__next__()
    [(4, 4, 4, 4), (5, 5, 5, 5)]
    """
    result_iterator = []
    for partition in partitions:
        result_iterator.append(grouper(partition))
    return iter(result_iterator)


def distinct_partition(partitions):
    """
    Function to remove duplicates in partitions.
    :param partitions:
    :type partitions: list[list]
    :return: Distinct data across partitions.

    >>> l = [[(1,2,3,4),(1,2,3,4)]]
    >>> it = distinct_partition(l)
    >>> it.__next__()
    [(1, 2, 3, 4)]
    """
    result_iterator = []
    for partition in partitions:
        result_iterator.append(list(set(partition)))
    return iter(result_iterator)


def is_function(line):
    """
    Function for checking if a line of code is a function or not.
    :param line: Current line of the source code.
    :type line: str
    :return: True/False

    >>> line1 = 'def func1():'
    >>> is_function(line1)
    True
    >>> line2 = 'def func2(a b, c):'
    >>> is_function(line2)
    True
    >>> line3 = 'a = 345'
    >>> is_function(line3)
    False
    """

    if 'def ' in line and '(' in line and ')' in line:
        return True
    return False


def is_loop(line):
    """
    Function to filter out loops in the source code.
    :param line: Line of code being examined.
    :type line: str
    :return: True/False
    """
    if 'for ' in line:
        return True
    return False


def not_empty(line):
    """
    Function to help filter none empty lines.
    :param line: Line of code
    :type line: str
    :return: True/False

    >>> line1 = '           '
    >>> not_empty(line1)
    False
    >>> line2 =''
    >>> not_empty(line2)
    False
    >>> line3 = 'a = 345'
    >>> not_empty(line3)
    True
    """
    if line.strip():
        return True
    return False


def is_package(line):
    """
    Function to determine if a line of code imports a package.
    :param line: Line of code.
    :type line: str
    :return: True/False

    >>> line1 = 'import math'
    >>> is_package(line1)
    True
    >>> line2 = 'from math import sqrt'
    >>> is_package(line2)
    True
    >>> line3 = 'a = 2**3'
    >>> is_package(line3)
    False
    """

    if line.strip():
        if line.startswith('import') or line.startswith('from'):
            return True
        return False
    return False


def nesting_factor(partitions):
    """
    Function to extract the loop nesting factor in repository.
    :param partitions: List of source code in different partitions.
    :type partitions: list[list]
    :return: A number corresponding to nesting_factor.

    #>>> s1 = ['for partition in partitions:','		for line in partition:', '				for plate in plates:']
    #>>> nesting_factor([s1])
    #2.0
    #>>> s2 = ['for partition in partitions:','		for line in partition:', 'for plate in plates:', '      for e in elements:']
    #>>> nesting_factor([s2])
    #1.0
    #>>> nesting_factor([s1, s2])
    #1.3333333333333333
    """

    nesting = []
    for partition in partitions:
        prev_indent = None
        count = 0
        for line in partition:
            indent = len(line)-len(line.lstrip())
            if prev_indent != None:
                if prev_indent < indent:
                    count = count + 1
                elif prev_indent > indent:
                    if count > 0:
                        nesting.append(count)
                    count = 0
                else:
                    count = count + 1
            prev_indent = indent
        nesting.append(count)
    # return functools.reduce(add, nesting)/len(nesting)
    return iter(nesting)


if __name__ == '__main__':

    # Check to see if correct argument number passed
    # if len(sys.argv) < 2:
    # print("Invalid directory specified!")
    # print("Usage: python3 Metrics.py DIR_NAME")
    # sys.exit(1)

    # import doctest
    # doctest.testmod()

    # Initialize Spark
    # Might be required if you are not running in the cloud.
    # findspark.init()

    parent_directory = REPO_ROOT
    # repositories = [repository for root, repository,
    #                files in os.walk(parent_directory) if repository][0]
    # print("# of repositories: ", len(repositories))

    sc = SparkContext(appName='Turing Data Pipeline')
    url_tuple = sc.textFile(CSV_SOURCE).map(lambda url: (url.split('/')[-1], url)).collect()
    url_dict = dict((str(x), str(y)) for x, y in url_tuple)
    repositories = url_dict.keys()
    result = []
    for repository in repositories:
        try:
            path = "{0}/{1}/*.py".format(REPO_ROOT_GS, repository)
            code_rdd = sc.textFile(path).filter(not_empty).cache()
            # file_count = code_rdd.getNumPartitions()
            lines_count = code_rdd.count()
            functions = code_rdd.filter(is_function)
            function_count = functions.count()
            parameter_count = 0

            try:
                functions.flatMap(extract_parameter).count()
            except:
                map(print, functions.collect())

            # Code duplicates
            # A naive approach
            rdd_group_four = code_rdd.mapPartitions(group_partition)
            code_duplicates = 100 * \
                              ((lines_count - rdd_group_four.mapPartitions(distinct_partition)
                                .count())/lines_count)

            # code_list = preprocessed_code.

            # Average of parameters per function definition in repository.
            avg_parameter = 0
            try:
                avg_parameter = parameter_count/function_count
            except:
                print('Error, Parameter count: {}'.format(parameter_count))

            packages = code_rdd.filter(is_package).flatMap(
                extract_package).distinct().collect()
            variable_count = code_rdd.flatMap(extract_variable).distinct().count()

            # Average number of variables defined per function definition repository.
            avg_variable = 0
            try:
                avg_variable = variable_count/lines_count
            except:
                pass

            # Nesting factor computation

            factor = 0
            try:
                loops_rdd = code_rdd.filter(is_loop).mapPartitions(nesting_factor).filter(lambda x: x > 0)
                factor = loops_rdd.sum()/loops_rdd.count()
            except:
                pass

            # Record metrics
            repository_result = dict()
            try:
                repository_result['repository_url'] = url_dict[repository]
                repository_result['number of line '] = lines_count
                repository_result['libraries'] = packages
                repository_result['nesting factor'] = factor
                repository_result['code duplication'] = code_duplicates
                repository_result['average parameters'] = avg_parameter
                repository_result['average variables'] = avg_variable
                result.append(repository_result)

                print('Result: {}'.format(repository_result))
            except:
                pass
        except:
            pass

    # sc.parallelize(result).saveAsTextFile(RESULT)

    with open('result.json', 'w+') as result_file:
        json.dump(result, result_file)

    sc.stop()
