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
        await self.config.mysql_pool_cli()  # init mysql pool

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

        await self.perform_write(responses)
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
        await self.config.cursor.execute("SHOW TABLES LIKE '%s'" % (self.config.name, ))
        result = await self.config.cursor.fetchone()
        if result is None:
            await self.create_table(responses)
        else:
            # check field
            await self.config.cursor.execute("DESC %s" % (self.config.name, ))
            results = await self.config.cursor.fetchall()
            fields = set(i["Field"] for i in results)
            self.key_fields = list(i["Field"] for i in results)
            real_keys = set(responses[0].keys())
            difference_set = real_keys.difference(fields)
            if difference_set:
                # real keys not subset of fields
                raise ValueError("Field %s not in MySQL Table: %s" % (str(difference_set), self.config.name))

    async def create_table(self, responses):
        sql = """
        CREATE TABLE '%s' (
        """
        for key, value in responses[0].items():
            if key in ("content", ) or isinstance(value, dict) or isinstance(value, list):
                field_type = "TEXT"
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
                field_type = "VARCHAR(%d)" % (len(value) * 4, )
            sql += "'%s' %s" % (key, field_type)
            if key == "id":
                sql += " NOT NULL,\n"
            else:
                sql += ",\n"

        tail_sql = """
        PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
        sql += tail_sql
        await self.config.cursor.execute(sql)
        await self.config.connection.commit()

    async def perform_write(self, responses):
        sql = "INSERT INTO %s VALUES " % (self.config.name, )
        for each in responses:
            curr_sql = '('
            for field in self.key_fields:
                val = each[field]
                if isinstance(val, dict) or isinstance(val, list):
                    val = json.dumps(val)
                curr_sql += repr(val) + ","
            curr_sql = curr_sql[:-1] + '),\n'
            sql += curr_sql

        try_time = 0
        while try_time < self.config.max_limit:
            try:
                await self.config.cursor.execute(sql)
                await self.config.cursor.connection.commit()
            except Exception as e:
                try_time += 1
                if try_time < self.config.max_limit:
                    logging.error("retry: %d, %s" % (try_time, str(e)))
                    await asyncio.sleep(random.randint(self.config.random_min_sleep, self.config.random_max_sleep))
                else:
                    logging.error("Give up MySQL writer: %s, After retry: %d times, still fail to write, "
                                  "total get %d items, total filtered: %d items, reason: %s" %
                                  (self.config.name, self.config.max_retry, self.success_count, self.total_miss_count,
                                   str(traceback.format_exc())))
