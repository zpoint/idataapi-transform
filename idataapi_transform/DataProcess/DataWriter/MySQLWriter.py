import json
import asyncio
import random
import logging
import traceback
from .BaseWriter import BaseWriter


class MySQLWriter(BaseWriter):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.total_miss_count = 0
        self.success_count = 0
        self.table_checked = False
        self.key_fields = list()

    async def write(self, responses):
        await self.config.get_mysql_pool_cli()  # init mysql pool

        miss_count = 0
        original_length = len(responses)
        if self.config.filter:
            target_responses = list()
            for i in responses:
                i = self.config.filter(i)
                if i:
                    target_responses.append(i)
                else:
                    miss_count += 1
            responses = target_responses

        if not responses:
            self.finish_once(miss_count, original_length)
            return

        # After filtered, still have responses to write
        if not self.table_checked:
            await self.table_check(responses)

        if await self.perform_write(responses):
            self.finish_once(miss_count, original_length)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.config.free_resource()
        logging.info("%s write done, total filtered %d item, total write %d item" %
                     (self.config.name, self.total_miss_count, self.success_count))

    def __enter__(self):
        return self

    def finish_once(self, miss_count, original_length):
        self.total_miss_count += miss_count
        self.success_count += original_length
        logging.info("%s write %d item, filtered %d item" % (self.config.name, original_length - miss_count, miss_count))

    async def table_check(self, responses):
        await self.config.cursor.execute("SHOW TABLES LIKE '%s'" % (self.config.table, ))
        result = await self.config.cursor.fetchone()
        if result is None:
            await self.create_table(responses)
        # check field
        await self.config.cursor.execute("DESC %s" % (self.config.table, ))
        results = await self.config.cursor.fetchall()
        fields = set(i[0] for i in results)
        self.key_fields = list(i[0] for i in results)
        real_keys = set(responses[0].keys())
        difference_set = real_keys.difference(fields)
        if difference_set:
            # real keys not subset of fields
            raise ValueError("Field %s not in MySQL Table: %s" % (str(difference_set), self.config.table))

        self.table_checked = True

    async def create_table(self, responses):
        test_response = dict()
        for response in responses[:50]:
            for k, v in response.items():
                if k not in test_response:
                    test_response[k] = v
                elif test_response[k] is None:
                    test_response[k] = v
                elif isinstance(v, dict) or isinstance(v, list):
                    if len(json.dumps(test_response[k])) < len(json.dumps(v)):
                        test_response[k] = v
                elif v is not None and test_response[k] < v:
                    test_response[k] = v

        sql = """
        CREATE TABLE `%s` (
        """ % (self.config.table, )
        first_field = True
        for key, value in responses[0].items():
            if "Count" in key:
                field_type = "BIGINT"
            elif value is None:
                field_type = "TEXT"
            elif key in ("content", ) or isinstance(value, dict) or isinstance(value, list):
                field_type = "TEXT"
            elif isinstance(value, bool):
                field_type = "BOOLEAN"
            elif isinstance(value, int):
                field_type = "BIGINT"
            elif isinstance(value, float):
                field_type = "DOUBLE"
            # varchar can store at most 65536 bytes, utf8 occupy 1-8 bytes per character,
            # so length should be less than 65536 / 8 = 8192
            #  assume this field  (the shortest length) * 4 <= the longest length(8192)
            elif len(value) > 2048:
                field_type = "TEXT"
            else:
                length = len(value) * 4
                if length < 256:
                    length = 256
                field_type = "VARCHAR(%d)" % (length, )
            sql += ("\t" if first_field else "\t\t") + "`%s` %s" % (key, field_type)
            if key == "id":
                sql += " NOT NULL,\n"
            else:
                sql += ",\n"
            if first_field:
                first_field = False

        tail_sql = """
        \tPRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=%s
        """ % (self.config.encoding, )
        sql += tail_sql
        logging.info("Creating table: %s\n%s", self.config.table, sql)
        await self.config.cursor.execute(sql)
        await self.config.connection.commit()
        logging.info("table created")

    async def perform_write(self, responses):
        sql = "REPLACE INTO %s VALUES " % (self.config.table, )
        for each in responses:
            curr_sql = '('
            for field in self.key_fields:
                val = each[field]
                if isinstance(val, dict) or isinstance(val, list):
                    val = json.dumps(val)
                if val is None:
                    curr_sql += 'NULL,'
                else:
                    curr_sql += repr(val) + ","
            curr_sql = curr_sql[:-1] + '),\n'
            sql += curr_sql
        sql = sql[:-2]
        try_time = 0
        while try_time < self.config.max_retry:
            try:
                await self.config.cursor.execute(sql.encode(self.config.encoding))
                await self.config.cursor.connection.commit()
                return True
            except Exception as e:
                try_time += 1
                if try_time < self.config.max_retry:
                    logging.error("retry: %d, %s" % (try_time, str(e)))
                    await asyncio.sleep(random.randint(self.config.random_min_sleep, self.config.random_max_sleep))
                else:
                    logging.error("Give up MySQL writer: %s, After retry: %d times, still fail to write, "
                                  "total write %d items, total filtered: %d items, reason: %s" %
                                  (self.config.name, self.config.max_retry, self.success_count, self.total_miss_count,
                                   str(traceback.format_exc())))
        return False
