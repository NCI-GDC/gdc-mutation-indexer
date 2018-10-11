#!/usr/bin/env python

"""
Convert Gistic files between columnar (wide) form and row-based (narrow) form.
"""

import argparse
import re


# The name of the column under which to write the aliquot IDs.
ID_COLUMN = 'aliquot_id'

# The name of the column under which to write the cell values.
VALUE_COLUMN = 'cnv_change'


def is_aliquot_id(str):
    """
    Check if a given string is formatted like an aliquot ID (i.e., a UUID).

    Args:
        str (str): The string to check.

    Returns:
        bool: Whether the string might be an aliquot ID.
    """
    # This may or may not be cleaner than checking if uuid(str) throws an
    # exception, but has the advantage of tolerating made-up test UUIDs that
    # don't set all of the bits according to spec.
    uuid_regex = '[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'
    return bool(re.search(uuid_regex, str))


def parse_row(row, delimiter):
    """
    Parse a row from a file. Split on the delimiter and trim newlines.

    Args:
        row (str): The raw row text, possibly with trailing newlines.
        delimiter (str): The delimiter to use to split columns.

    Returns:
        list: The parsed row cells.
    """
    cells = row.split(delimiter)
    cells[-1] = cells[-1].rstrip('\n\r')
    return cells


def parse_columnar_header(row, delimiter):
    """
    Parse the column names from a file header.

    Separate the names of metadata columns from the aliquot IDs. Return each
    list of columns in the same order in which they appear in the header.
    Aliquot IDs are identified by is_aliquot_id; all other columns are
    assumed to be metadata columns.

    Args:
        row (str): The raw row text.
        delimiter (str): The delimiter to use to split columns.

    Returns:
        tuple: (list of metadata column names, list of aliquot IDs)
    """
    columns = parse_row(row, delimiter)

    meta_columns = []
    aliquot_ids = []
    for column in columns:
        if is_aliquot_id(column):
            aliquot_ids.append(column)
        else:
            meta_columns.append(column)

    return meta_columns, aliquot_ids


def write_row(output, values, delimiter):
    """
    Write a row to an output stream, with appropriate delimiters.

    Args:
        output (File): The file to which to write.
        values (list): The values to write.
        delimiter (str): The column delimiter to write between values.
    """
    output.write(delimiter.join(values))
    output.write('\n')
    pass


def columns_to_rows(input, output, delimiter, filter_zeroes):
    """
    Parse a columnar Gistic file and output it in row-based format.

    Assume the input file is formatted like the following:

    Gene   Metadata_1   Metadata_N   Aliquot_ID_1   Aliquot_ID_2   Aliquot_ID_N
    gene1         ...          ...              0              1             -1
    gene2         ...          ...             -1              2             -2

    "Unpivot" the aliquot ID columns and write out one value per row:

    Gene   Metadata_1   Metadata_N   aliquot_id   cnv_change
    gene1         ...          ...   Aliquot_ID_1          0
    gene1         ...          ...   Aliquot_ID_2          1
    gene1         ...          ...   Aliquot_ID_N         -1
    gene2         ...          ...   Aliquot_ID_1         -1
    gene2         ...          ...   Aliquot_ID_2          2
    gene2         ...          ...   Aliquot_ID_N         -2

    Args:
        input (File): The file from which to read.
        output (File): The file to which to write.
        delimiter (str): The column delimiter to expect.
        filter_zeroes (bool): Whether to omit zero-valued rows from the output.
    """
    meta_columns, aliquot_ids = parse_columnar_header(input.next(), delimiter)
    num_meta_columns = len(meta_columns)
    num_columns = num_meta_columns + len(aliquot_ids)

    write_row(output, meta_columns + [ID_COLUMN, VALUE_COLUMN], delimiter)

    line_number = 1
    for row in input:
        cells = parse_row(row, delimiter)

        # If a row is missing cells for certain aliquots, we should identify
        # the problem and get a better data file ASAP, so be loud here.
        if len(cells) != num_columns:
            raise ValueError('Line {} has {} columns; expected {})'.format(
                line_number, len(cells), num_columns))

        meta_values = cells[:num_meta_columns]
        values = cells[num_meta_columns:]

        for i, aliquot_id in enumerate(aliquot_ids):
            value = values[i]
            if value != '0' or not filter_zeroes:
                write_row(output, meta_values + [aliquot_id, value], delimiter)

        line_number += 1


def parse_args():
    """
    Parse command line arguments.

    Returns:
        Namespace: The parsed arguments.
    """
    description = 'Converts Gistic files between columnar and row-based form'
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(
        'input', type=argparse.FileType('r'),
        help='input filename'
    )

    parser.add_argument(
        '-o', '--output', type=argparse.FileType('w'), default='-',
        help='output filename'
    )

    parser.add_argument(
        '-d', '--delimiter', default='\t',
        help='column delimiter string (default tab)'
    )

    parser.add_argument(
        '-z', '--keep-zeroes', dest='filter_zeroes', action='store_false',
        help='keep zero-valued CNVs in the output')

    args = parser.parse_args()

    return args


def main():
    """
    Main program driver.
    """
    args = parse_args()

    try:
        columns_to_rows(
            args.input, args.output, args.delimiter, args.filter_zeroes)
    finally:
        args.input.close()
        args.output.close()


if __name__ == '__main__':
    main()
