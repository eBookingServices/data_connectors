from table import Table
from re import sub, match
import logging
from sys import stdout
from typing import Generator

logging.basicConfig(stream=stdout, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MySQLTable(Table):
    """
    This class is intended to help interact with MySQL using Python code. Its
    most notable methods, as in methods that can be of great help when
    interacting with MySQL, are:
        - get_rows
        - initialize
    """

    def __init__(self, connection, schema: str, table: str,
                 cols_to_hash: list=[], computed_cols: list=[]):
        """
        Initialization method of the MySQLTable class
        :param connection: psycopg2 connection to a MySQL instance.
        :param schema: Schema name where the table is to be found.
        :param table: Table name.
        :param cols_to_hash: If the user wants to hash a specific column
        content. Useful when PII is present in the data.
        :param computed_cols:
        """
        super().__init__(connection)
        self.conn = connection
        self.schema = schema
        self.name = table
        self.columns = []
        self.colnames = None
        self.rows = None
        self.cols_to_hash = cols_to_hash
        self.computed_cols = computed_cols


    @staticmethod
    def _format_row_type(row: tuple) -> dict:
        """
        This method receives the row types as returned by MySQL and generates a
        dictionary with all required information.
        :param row: tuple containing the definition of a column as returned by
        MySQL.
        :return: A dictionary as the following
        {
            "colname": "name of the column. E.g. organizer_id",
             "type": "data type contained in the column. E.g. INTEGER",
             "nullable": "boolean indicating whether the column allows for NULL
             values",
             "key": "Indicates whether this column is a primary key",
             "default": "Default value of the column, if present",
             "extra": "extra information returned by MySQL",
             "mysql_type": "the data type returned by MySQL e.g. INTEGER",
             "size": "Size of the field, e.g. 255 for a VARCHAR(255). -1 when
             not applicable",
             "sign": "Negative or Positive values",
             "precision": "Precision of the NUMERIC or DECIMAL type. -1 when not
             applicable"
        }
        """
        # Cols returned by MySQL
        exp_cols = ["colname", "type", "nullable", "key", "default", "extra"]

        # Transform to a dict
        out = dict(zip(exp_cols, row))
        out["mysql_type"] = out["type"]
        out["size"] = None
        out["sign"] = None
        out["precision"] = None

        # Transform bigint(20) to {"type": "biging", "size": 20}
        r = "([a-z]+)(\()([0-9]+\,*[0-9]*)(\))\s{0,1}([a-z]{0,})"
        if match(r, out["type"]):
            field_type = sub(r, "\\1", out['type'])
            size = sub(r, "\\3", out['type']).split(",")
            precision = int(size[1]) if len(size) > 1 and size[1].isdigit() \
                else \
                None
            size = size[0]
            sign = sub(r, "\\5", out['type'])

            out['size'] = int(size) if size.isdigit() else -1
            out["type"] = field_type
            out["precision"] = precision
            out["sign"] = sign
        elif "enum" in out["type"]:
            out["type"] = "enum"
        elif "set" in out["type"]:
            out["type"] = "set"

        return out

    def _get_row_types(self) -> None:
        """
        This method executes a DESCRIBE function on the table under
        consideration and calls :meth _format_row_type to format correctly the
        information. It updates the :attr columns.
        :return: None
        """
        sql = """describe {}.{}""".format(self.schema, self.name)
        logger.debug("Executing\n {}".format(sql))
        resp = self.execute_query(sql)
        for r in resp:
            self.columns.append(self._format_row_type(r))

            if len(self.computed_cols) >= 1:
                for c_col in self.computed_cols:
                    if r[0] == c_col['after']:
                        self.columns.append(self._format_row_type((
                            # exp_cols = ["colname", "type", "nullable", "key", "default", "extra"]
                            c_col['name'],
                            c_col['type'],
                            c_col.get('nullable', 'YES'),
                            c_col.get('key', ''),
                            c_col.get('default', None),
                            c_col.get('extra', ''),
                        )))

    def _prepare_colnames(self) -> None:
        """
        This method simply creates a list containing the names of the columns
        present in the table.
        :return: None.
        """
        self.colnames = [c['colname'] for c in self.columns]

    def _get_cols_for_select(self) -> str:
        """
        This method simply generates the list of columns required for the SELECT
        statement. When required, it also wrap the columns that need hashing
        with SHA2(<column>, 256).
        :return: String containing the list of columns to be selected.
        """
        cols_for_select = []
        for colname in self.colnames:
            if colname in self.cols_to_hash:
                cols_for_select.append('SHA2(`{}`, 256)'.format(colname))
            else:
                cols_for_select.append('`{}`'.format(colname))

            if len(self.computed_cols) >= 1:
                for c_col in self.computed_cols:
                    if colname == c_col['name']:
                        cols_for_select.pop()
                        cols_for_select.append('{}'.format(c_col['expression']))

        return ','.join(cols_for_select)

    def initialize(self) -> None:
        """
        Utility function to initialize the definition of the table under
        consideration. It will:
            - Get the types fo all the columns in the table
        :return: None
        """
        self._get_row_types()
        self._prepare_colnames()

    def get_rows(self, cursor, limit: int=0) -> Generator:
        """
        This method generates and executes a SELECT statement against the table
        under consideration.
        :param cursor: This is the cursor we want the function to use. It is
        passed as an argument to allow the function to return a Generator, for a
        better management of the available RAM.
        :param limit: Integer indicating how many rows we want to retrieve.
        Default value is 0 and means no limit.
        :return: Yields a list of tuples containing the data from MySQL.
        """
        sql = """SELECT
            {cols}
            from {schema}.{table}
            """.format(
            cols=self._get_cols_for_select(),
            schema=self.schema,
            table=self.name)
        if limit:
            sql += " LIMIT {}".format(limit)

        logger.debug("Get rows with \n {}".format(sql))
        # Make sure the timeout is 120s - Hope is it fixes the 'lost connection'
        # issue
        #cursor.execute("SET NET_WRITE_TIMEOUT = 120")
        cursor.execute(sql)
        while True:
            logger.debug("while true loop")
            res = self.conn.get_rows(200000)
            logger.debug("got rows")
            if not res:
                break
            for r in res:
                yield r

