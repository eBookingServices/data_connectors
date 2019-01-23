import logging
from sys import stdout
import psycopg2
from table import Table
from csv import QUOTE_NONNUMERIC, QUOTE_ALL
from datetime import datetime
from test_suite import Test
from json import load
from typing import Union
from pandas import DataFrame, read_sql
from numpy import dtype

logging.basicConfig(stream=stdout, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


class RedShiftTable(Table):
    """
    This class is intended to help interact with Redshift using Python code. Its
    most notable methods, as in methods that can be of great help when
    interacting with Redshift, are:
        - get_rows
        - insert_pandas
        - create_as_select
    """
    NA_REP = 'MYNULL'

    def __init__(self, conn, table_name: str = None, columns: list = None,
                 dry_run: bool = False, compound_sort=False,
                 access_key: str,
                 secret_key: str,
                 sortkey: tuple = (), distkey: tuple = (),
                 diststyle: str = "key",
                 debug: bool = False):
        """
        Initialization script for the class RedshiftTable.
        :param conn: psycopg2 connection to the database.
        :param table_name: The table name as you would like it to appear in
        Redshift. This name should contain the schema (if required). Default
        value is None.
        :param columns: Dictionary containing the description of the columns.
        For example:
                columns = [
                    {
                        "colname": "testcol",
                        "type": "VARCHAR",
                        "size": 65535,
                        "precision": -1
                    }
                ]
        Default value is None as its value is only required when we want to
        created the table.
        :param dry_run: Boolean to indicate whether we want the query statements
        that will be generated/passed to be executed or not. Default value is
        False.
        :param access_key: Access key to upload data to S3.
        :param secret_key: Secret key to upload data to S3.
        :param sortkey: Tuple containing all sort keys for the currenet table.
        Default value is an empty tuple.
        :param distkey: Tuple containing all dist keys for the currenet table.
        Default value is an empty tuple.
        :param disttype: Disttype for the distkeys. Default value is `KEY`
        :param debug: Boolean to indicate whether we want DEBUG or INFO level
        logging.
        """

        super().__init__(conn)

        if not isinstance(sortkey, tuple) or not isinstance(distkey, tuple):
            raise ValueError("sortkey and distkey are required to be tuples.")

        self.con = conn
        self.redshift_table = table_name
        self.columns = columns
        self.dry_run = dry_run
        self.access_key = access_key
        self.secret_key = secret_key
        self.sortkey = sortkey
        self.distkey = distkey
        self.diststyle = diststyle
        self.compound_sort = compound_sort
        if debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

    def _drop_table(self, suffix: str = "") -> None:
        """
        Simply drops a table, after checking for its existence.
        :param suffix: String parameter that is used as a suffix to the table
        name. This is to allow working on `temporary` versions of the table.
        we're working on.
        :return: None.
        """
        sql = """DROP TABLE IF EXISTS {}{};""".format(
            self.redshift_table, suffix)
        logger.debug("Drop table with = \n" + sql)
        self.execute_query(sql, fetch=False)

    def _get_create_statement(self, suffix: str = "") -> str:
        """
        Generates a create table statement with all correct column types,
         SORTKEY, DISTKEY and DISTSTYLE.
        :param suffix: String parameter that is used as a suffix to the table
        name. This is to allow working on `temporary` versions of the table.
        :return: The query statement itself.
        """
        if self.columns is None:
            raise ValueError("columns are not defined.")

        out = "CREATE TABLE IF NOT EXISTS {}{}" \
              "(\n".format(self.redshift_table, suffix)

        tmp = []
        for c in self.columns:
            if c['size'] != -1 and c['precision'] != -1:
                tmp.append('"{}" {}({},{})'.format(c['colname'], c['type'],
                                                   c['size'], c['precision']))

            elif c['size'] != -1 and c['precision'] == -1:
                tmp.append('"{}" {}({})'.format(c['colname'],
                                                c['type'], c['size']))

            elif c['size'] == -1:
                tmp.append('"{}" {}'.format(c['colname'], c['type']))

        out += ",".join(tmp)
        out += ")\n"

        if self.diststyle == "ALL" and len(self.distkey):
            raise ValueError("Diststyle ALL doesn't allow for a distkey")

        if len(self.distkey) > 0:
            if self.diststyle is not None:
                out += "DISTSTYLE {}\n".format(self.diststyle)
            out += "distkey({})\n".format(",".join(self.distkey))

        if self.diststyle == "ALL":
            out += "DISTSTYLE {}\n".format(self.diststyle)

        if len(self.sortkey) > 0:
            if self.compound_sort:
                out += 'compound '

            out += "sortkey({})\n".format(",".join(self.sortkey))

        out += ";"
        return out

    def _get_create_like_table(self, original: str) -> str:
        """
        Generates a SQL statement to allow to create the current table as a
        copy (in terms of column names, distkeys...) of the :param original
        table.
        :param original: Name of the table we want to base the create statement
        on.
        :return: The query statement.
        """
        sql = "CREATE TABLE {copycat} LIKE {original}"
        return sql.format(copycat=self.redshift_table, original=original)

    def add_user_right(self, right: str = None, users: list = []) -> None:
        """
        Generates and executes an SQL query to provide a list of users a
        specific right on the current table.
        :param right: The permission we want to grant.
        :param users: The users we want to grant a permission.
        :return: None.
        """
        sql = """GRANT {} ON TABLE {} TO {};""".format(
            right, self.redshift_table, ','.join(users))
        logger.debug("Grant rights to table")
        self.execute_query(sql, fetch=False)

    def create_table(self, like: bool = False,
                     original: str = None, suffix: str = "") -> None:
        """
        Generates and executes a CREATE TABLE statement.
        :param like: Boolean to indicate whether we want to create the table by
        using another one (create LIKE).
        :param original: If :param  like is True, this parameter indicates the
        name of the table we want to imitate.
        :param suffix: String parameter that is used as a suffix to the table
        name. This is to allow working on `temporary` versions of the table.
        :return: None.
        """
        if like:
            create_sql = self._get_create_like_table(original)
        else:
            create_sql = self._get_create_statement(suffix)

        logger.debug("Create table with \n" + create_sql)
        self.execute_query(create_sql, fetch=False)

    def copy(self, s3path: str, suffix: str, file_format: str,
             column_order=None) -> None:
        """
        Generates and executes the COPY statement to load data from S3 to the
        current table.
        :param s3path: S3 path where the data files are stored.
        :param suffix: String parameter that is used as a suffix to the table
        name. This is to allow working on `temporary` versions of the table.
        :param file_format: CSV of JSON.
        :param column_order: list of columns as they appear in the data
        :return: None
        """
        if self.access_key is None or self.secret_key is None:
            raise ValueError("Both access_key and secret must be set before "
                             "running a copy from S3")

        if column_order is not None:
            cols = "("+",".join(column_order)+")"
        else:
            cols = ""

        copy_sql = """
            COPY {}{} {}
            FROM 's3://{}'
            credentials 'aws_access_key_id={};aws_secret_access_key={}'
            ACCEPTINVCHARS
            NULL AS '{}'
            {}""".format(self.redshift_table, suffix, cols, s3path,
                         self.access_key, self.secret_key, self.NA_REP,
                         file_format)
        if format == "JSON":
            copy_sql += " 'auto';"
        else:
            copy_sql += ";"

        logger.debug("Copy table with \n" + copy_sql)
        self.execute_query(copy_sql, fetch=False)

    @staticmethod
    def _alter(o: str, t: str) -> str:
        """
        Generates an ALTER TABLE RENAME statement.
        :param o: The table we want to rename.
        :param t: The new name of the table.
        :return: The SQL statement.
        """
        import re
        r = re.compile('^([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)$')
        m = r.match(t)
        if m:
            # Grab the second part of the tablename (the first part is the
            # schema)
            t = m.group(2)
        rename_sql = """ALTER TABLE {} RENAME TO {}; """.format(o, t)
        logger.debug("Alter table with \n" + rename_sql)
        return rename_sql

    def _make_tables_dance(self) -> None:
        """
        Generates and exeecutes the statements for a table dance.
        :return: None.
        """
        self.con.commit()
        try:
            self.execute_query(
                self._alter(self.redshift_table, self.redshift_table + "_old"),
                fetch=False)
            drop_old = True
        except psycopg2.ProgrammingError as ex:
            logger.warning("Caught exception when renaming old table. "
                           "We'll assume it didn't exist. \n"
                           "Exception stated {}".format(ex))
            drop_old = False
            self.con.rollback()

        self.execute_query(
            self._alter(self.redshift_table + "_temp", self.redshift_table),
            fetch=False)

        if drop_old:
            self._drop_table("_old")

    def _check_row_count(self) -> int:
        """
        Generates and exectes a count(*) on the current table.
        :return: An integer indicating how many rows are in the current table.
        """
        sql = "SELECT COUNT(*) FROM {}".format(self.redshift_table)
        logger.debug("Count N rows with \n" + sql)
        return self.execute_query(sql, fetch=True)[0][0]

    def _truncate(self) -> None:
        """
        Generates and executes a TRUNCATE statement.
        :return: None
        """
        sql = "TRUNCATE {}".format(self.redshift_table)
        logger.debug("Running the query {}".format(sql))
        return self.execute_query(sql, fetch=False)

    def create_as_select(self, query: str, checkrowcount: bool = True,
                         recreate: bool = False) -> Union[None, int]:
        """
        Generates and executes a `CREATE TABLE AS SELECT` statement.
        :param query: The query that is used to create the table. Be aware that
        this query should only contain the `SELECT` statement without any
        semi-colon at the end. The `CREATE TABLE AS` and distkey, sortkeys are
        taken care of by the method itself.
        :param checkrowcount: Boolean to indicateed whether we want the method
        to execute a `count(*)` on the table after it was created. Default True.
        :param recreate: Boolean to indicate whether the table should be dropped
        before the `CREATE` statement is generated.
        :return: If the :param checkrowcount is True, the function returns the
        number of rows that the table contains after it was created. Otherwise,
        it will return None.
        """
        if recreate:
            self._drop_table()

        self._drop_table(suffix="_temp")
        sql = "CREATE TABLE {}_temp\n".format(self.redshift_table)

        if len(self.sortkey) > 0:
            if self.compound_sort:
                sql += 'COMPOUND '
            sql += "SORTKEY ({})".format(",".join(self.sortkey))

        if len(self.distkey) > 0:
            sql += "DISTKEY ({})".format(",".join(self.distkey))

        sql += "AS {};".format(query)
        logger.debug("Running the query {}".format(sql))
        try:
            self.execute_query(sql, fetch=False)
            self._make_tables_dance()

        finally:
            # No matter what, we should drop the _temp table
            self._drop_table("_temp")

        if checkrowcount:
            return self._check_row_count()

    def get_rows(self, sql: str = None, dataframe: bool = True,
                 chunksize: int = None, cols: tuple = ('*',),
                 limit: int = None) -> Union[list, DataFrame]:
        """
        Function to execute a SQL statement and return its result. You can abuse
        it to execute SQL statements on other tables, as long as they're
        reachable with the same connection object.
        :param sql: SQL statement to execute on the table. When None, a simple
        `SELECT *` or `SELECT <column_names>` (when :param cols is provided) is
        executed on the current table. The statement should not end with a semi-
        colon. Default value is None.
        :param dataframe: Boolean to indicate whether we want the results to be
        returned as a pandas DataFrame or a list of tuples. True by default.
        :param chunksize: This is a parameter that is passed directly to the
        method `pandas.read_sql`. Default value is None.
        (from pandas docs) If specified, the function will return an iterator
        where `chunksize` is the number of rows to include in each chunk.
        :param cols: Tuple indicating which columns we want to SELECT. Used only
        when the :param sql is None. Default value is None.
        :param limit: Integer indicating how many rows we want to retrieve.
        :return: Pandas DF or list of tuples containing the requested data.
        Default value is None.
        """
        if sql is None:
            sql_ = "SELECT {cols} \nFROM {table_name}".format(
                cols=", \n".join(cols),
                table_name=self.redshift_table
            )
        else:
            sql_ = sql
        if limit is not None:
            sql_ += " LIMIT {}".format(limit)

        if dataframe:
            return read_sql(sql_, self.con, chunksize=chunksize)
        else:
            return self.execute_query(sql_, fetch=True)

    def _rows_to_csv(self, df: DataFrame, quoting=QUOTE_NONNUMERIC,
                     escapechar='\\', quotechar="'", header=False) -> str:
        """
        Converts a pandas DataFrame to a string where the values are comma
        separated. Under the hood, this method makes use of the method `to_csv`
        of pandas DataFrames and the package StringIO.
        :param df: DataFrame we want to convert to a CSV string.
        :param quoting: Option of the CSV package (used by the pandas method
        `to_csv`). Controls how the cells are quoted. By default, its value is
        set to csv.QUOTE_NONNUMERIC
        :param escapechar: Option of the CSV package (used by the pandas method
        `to_csv`). Controls which charecter is used to escape special
        characeters. Default value is '\\'.
        :param quotechar:  Option of the CSV package (used by the pandas method
        `to_csv`). Controls which character is used to quote (when needed)
        values. Default value is "'"
        :param header: Controls whether the header should be written in the CSV.
        :return: A string where rows occupy 1 line and values inside rows are
        separated by commas.
        """
        from io import StringIO
        to_write = []
        s = StringIO(
            df.to_csv(index=False, quoting=quoting, quotechar=quotechar,
                      na_rep=self.NA_REP, escapechar=escapechar,
                      header=header))
        for line in s:
            to_write.append('(' +
                            line.replace(
                                "'{}'".format(self.NA_REP), "NULL")
                            + ')')

        return ',\n'.join(to_write)

    @staticmethod
    def upload_to_s3(infile: str, target_filename: str, date_prefix: str,
                     bucket: str = 'redshift-sql-pipeline',
                     delete: bool = False) -> str:
        """
        Function to upload data files to an S3 bucket.
        :param infile: Path to the data file.
        :param target_filename: Filename we want to use for the S3 bucket.
        :param date_prefix: Used to version data files on S3 based on the given
        date.
        :param bucket: S3 Bucket name. Default value is `redshift-sql-pipeline`
        :param delete: Boolean to indicate whether the file on disk should be
        deleted after the upload.
        :return: S3 path where the file was stored.
        """
        from boto3 import client
        from os import remove
        s3 = client('s3')

        s3_path_ = "{}/{}".format(date_prefix, target_filename)
        with open(infile, 'rb') as dumpfile:
            s3.upload_fileobj(dumpfile, Bucket=bucket, Key=s3_path_)
        if delete:
            remove(infile)

        return bucket + "/" + s3_path_

    def insert_pandas(self, df: DataFrame, create: bool = False,
                      recreate: bool = False, use_s3: bool = False,
                      merge: bool = False, column_order=True) -> int:
        """
        Function to insert the data of a pandas DataFrame into a table on
        Redshift.
        :param df: The DataFrame containing the data.
        :param create: Boolean to indicate whether we should create the table
        before inserting the data. Default value is False.
        :param recreate: Boolean to indicate whether we want the table to be
        dropped before inserting data into it. Default value is False.
        :param use_s3: Boolean whether we should use S3 to insert the data. This
        is useful when the data is too large to be inserted using an INSERT
        statement. Default value is False.
        :param merge: Boolean value to indicate whether the data contained in
        the pandas DataFrame should be combined with the data in the target
        table or not. The default value is False.
        :param: column_order: boolean that indicates whether we should pay
        attention to the column order when inserting the values into the table
        :param column_order: Boolean that indicates whether we should keep track
        of the column order when inserting the data into the table
        :return: Number of rows that were inserted into the table.
        """
        if recreate:
            self._drop_table()
            create = True

        if create:
            self.create_table()

        if use_s3:
            fname = '{}_temp.csv'.format(self.redshift_table)
            df.to_csv('/tmp/' + fname, index=False, header=False,
                      na_rep=self.NA_REP, quoting=QUOTE_ALL,
                      escapechar='\\', quotechar='"', encoding='utf-8')
            formatted_date = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
            s3_path = self.upload_to_s3("/tmp/" + fname, fname, formatted_date)
            if column_order:
                self.insert_rows(s3_path, "CSV", merge=merge,
                                 column_order=df.columns)
            else:
                self.insert_rows(s3_path, "CSV", merge=merge)

        else:
            rows = self._rows_to_csv(df)

            if column_order:
                self.insert_values(rows, column_order=df.columns, merge=merge)
            else:
                self.insert_values(rows, merge=merge)

        return self._check_row_count()

    def insert_values(self, val: str, column_order=None, merge=True) -> None:
        """
        Generates and executes an `INSERT INTO <table_name> VALUES
        <list of values>` statement.
        :param val: CSV representation of the data we want to insert.
        :param: column_order: list of columns as they appear in the data to be
        inserted
        :param column_order: List of columns as they appear in the data to be
        inserted into the table
        :param merge: Boolean to indicate whether we want to merge the current
        dataframe with the table content
        :return: None
        """

        column_order_ = ', '.join(column_order)

        self._drop_table(suffix="_temp")
        self.create_table(suffix="_temp")

        sql = "INSERT INTO {} ({}) VALUES {}".format(
                self.redshift_table+"_temp", column_order_, val
            )
        self.execute_query(sql, fetch=False)

        if merge:
            self.insert_into('select {} '
                             'from {}_temp'.format(column_order_,
                                                   self.redshift_table),
                             column_order)
            self._drop_table("_temp")
        else:
            logger.info("Table dance!")
            self._make_tables_dance()

    @staticmethod
    def _parse_col_order(column_order):
        if column_order is None:
            return ""
        else:
            return '(' + ','.join(column_order) + ')'

    def insert_into(self, select_statement: str,
                    column_order: list = None) -> int:
        """
        Generates and executes an `INSERT INTO <table_name> SELECT ...`. This
        is used for example to append new rows to an existing table.
        :param select_statement: Select statement to generate the new rows.
        :param: column_order: List of columns in the order they appear in the
        data
        :param column_order: List of columns as they appear in the data to be
        inserted into the table
        :return: Integer counting the number of rows in the target table.
        """
        col_order = self._parse_col_order(column_order)

        sql = "INSERT INTO {} {}" \
              "({})".format(self.redshift_table, col_order, select_statement)
        logger.debug("Running the query {}".format(sql))

        self.execute_query(sql, fetch=False)
        return self._check_row_count()

    def insert_rows(self, s3path, data_format, merge=False, column_order=None):
        logger.debug("  1- Drop old table")
        self._drop_table("_temp")
        logger.debug("  2- Create new temp table")
        self.create_table(suffix="_temp")
        logger.debug("  3- Copy the data")
        self.copy(s3path, "_temp", data_format, column_order)

        if column_order is None:
            colorder = '*'
        else:
            colorder = ','.join(column_order)

        if merge:
            n = self.insert_into('select {} '
                             'from {}_temp'.format(colorder,
                                                   self.redshift_table),
                             column_order)
            self._drop_table("_temp")
            return n
        else:
            logger.info("Table dance!")
            self._make_tables_dance()
            return self._check_row_count()

    def infer_columns(self, df: DataFrame) -> None:
        """
        Utility function that populates the attribute `:attr columns` of this
        object. It uses the mapping between Pandas data types and Redshift
        data types provided by the method :meth _pd_to_redshit_col.
        :param df: DataFrame we want to infer the column names and types from.
        :return: None
        """
        self.columns = []
        for col in df.columns:
            out = self._pd_to_redshit_col(df[col].dtype)
            out['colname'] = col
            self.columns.append(out)

    def run_tests(self, test_file: str) -> None:
        """
        This function reads and executes the test queries. It is used to make
        sure the `reporting_` tables always contain correct data. The tests are
        executed sequentially and an exception is raised when one fails.
        :param test_file: Name of the file (with extension, without the entire
        path) containing the test definitions.
        :return: None
        """
        with open("../test_queries/{}".format(test_file)) as f:
            tests = load(f)

        for t in tests:
            test = Test(t)
            test.parse_test_result(self.get_rows(test.test_query))

    @staticmethod
    def _pd_to_redshit_col(pandas_type: dtype) -> dict:
        """
        This method tries to map pandas data types to Redshit data types. It is
        however not efficient but can be handy when we don't want to bother with
        the types. The current mapping is as follows:
            [np.float, np.float32, np.float64] -> DECIMAL(20,8)
            [np.int, np.int32, np.int64] -> BIGINT
            np.object -> VARCHAR (65535)
            np.dtype('<M8[ns]') -> TIMESTAMP
        Raise an ValueError when the dtype is not known.
        :param pandas_type: dtype of the pandas DataFrame column under
        consideration.
        :return: A dictionary with all required information to create the table
        is provided.
        """
        import numpy as np
        if pandas_type in [np.float, np.float32, np.float64]:
            return {"type": "decimal", "precision": 8, "size": 20}
        elif pandas_type in [np.int, np.int32, np.int64]:
            return {"type": "BIGINT", "precision": -1, "size": -1}
        elif pandas_type == np.object:
            return {"type": "VARCHAR", "precision": -1, "size": 65535}
        elif pandas_type == np.dtype('<M8[ns]'):
            return {"type": "timestamp", "precision": -1, "size": -1}
        else:
            raise ValueError("I don't know how to translate pandas type {} to"
                             "a Redshift type".format(pandas_type))

