import json
import asyncio
import argparse
from .DataProcess.Config.DefaultValue import DefaultVal
from .DataProcess.Config.ConfigUtil import GetterConfig
from .DataProcess.Config.ConfigUtil import WriterConfig
from .DataProcess.ProcessFactory import ProcessFactory


class Args(object):
    from_choices = ["API", "ES", "CSV", "XLSX", "JSON", "REDIS", "MYSQL", "MONGO"]
    from_desc = "argument 'from' can only set to one of 'API', 'ES', 'CSV', 'XLSX', " \
                "'JSON'(means json line by line file), 'REDIS', 'MYSQL' or 'MONGO'"

    to_choices = ["csv", "xlsx", "json", "txt", "es", "redis", 'mysql', 'mongo']
    to_desc = "argument 'to' can only set to one of \"csv\", \"xlsx\", \"json\", \"txt\" \"es\", \"json\", \"redis\", " \
              "\"mysql\", \"mongo\", \"json\" will write 'json.dumps(item)' line by line. " \
              "\"txt\" will write each item line by line, each element in each line is separated by 'space' bu default"

    source_desc = """
    argument 'source', When argument '-from' set to 'ES', source should be 'index:doc_type' When 
    argument 'from' set tp 'API', source should be 'http://...
    argument 'from' set tp 'REDIS', source should be key name
    argument 'from' set tp 'MYSQL', source should be table name
    argument 'from' set to others, source should be file path
    """
    dest_desc = "argument 'dest', filename to save result, no need for suffix, " \
                "ie '/Desktop/result', default: './result'\n" \
                "When argument '-to' set to 'ES', dest should be 'index:doc_type'"

    per_limit_desc = "amount of data buffered, when buffer filled, Program will write buffered data to 'dest', default 100"
    max_limit_desc = "write at most 'max_limit' data to 'dest', if 'max_limit' set to 0, means no limit, default to None"
    retry_desc = "when fetch data failed, retry at most 'retry' time, default 3"
    r_encoding_desc = "encoding of input file, ignore for xlsx format, default 'utf8'"
    w_encoding_desc = "encoding of output file, ignore for xlsx format, default 'utf8'"

    filter_desc = "file contains a 'my_filter(item)' function for filter"

    param_file_desc = """When you have many item save in id.json, --param_file './id.json::id::pid' means open './id.json
    ', read each json object line by line, use each_json['id'] as the parameter 'pid' and add it to the tail part of 
    'source'. --param_file can be either "filename.json::json_param::request_param" or "filename.txt::request_param"
    """

    expand_desc = """If your item is {"a": {"b": "c"}, "b": "d"}, --expand 1 will make your item become 
    {"a_b": "c", "b": "d"}, --expand N means expand at most N level deep of your object, --expand -1 means expand all 
    level -- expand 0 means no expand of your item. Default 0.
    """
    qsn_desc = """quote scientific notation, ie: 4324234234234234123123 will become 4.32423423423423E+021 in normal csv, 
    If quote like '4324234234234234123123', it won't become scientific notation, Only work for output format 'csv' 
    --qsn True means quote scientific notation,  --qsn False means not quote scientific notation"""

    query_body_desc = """ElasticSearch query body, size has same function as "--limit", i.e: 
        body = {
            "size": 100,
            "_source": {
                "includes": ["location", "title", "city", "id"]
            },
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {"appCode": {"value": "ctrip"}}
                        }
                    ]
                }
            }
        }
    """

    write_mode_desc = """'w' or 'a+'"""
    key_type_desc = """redis data type to operate, options: [LIST] or [HASH], default: [LIST]"""


getter_config_map = {
    Args.from_choices[0]: GetterConfig.RAPIConfig,
    Args.from_choices[1]: GetterConfig.RESConfig,
    Args.from_choices[2]: GetterConfig.RCSVConfig,
    Args.from_choices[3]: GetterConfig.RXLSXConfig,
    Args.from_choices[4]: GetterConfig.RJsonConfig,
    Args.from_choices[5]: GetterConfig.RRedisConfig,
    Args.from_choices[6]: GetterConfig.RMySQLConfig,
    Args.from_choices[7]: GetterConfig.RMongoConfig
}

writer_config_map = {
    Args.to_choices[0]: WriterConfig.WCSVConfig,
    Args.to_choices[1]: WriterConfig.WXLSXConfig,
    Args.to_choices[2]: WriterConfig.WJsonConfig,
    Args.to_choices[3]: WriterConfig.WJsonConfig,
    Args.to_choices[4]: WriterConfig.WESConfig,
    Args.to_choices[5]: WriterConfig.WRedisConfig,
    Args.to_choices[6]: WriterConfig.WMySQLConfig,
    Args.to_choices[7]: WriterConfig.WMongoConfig
}


def get_arg():
    parser = argparse.ArgumentParser(prog="idataapi_transform",
                                     description='convert data from a format to another format, '
                                                 'read/write from file or database, suitable for iDataAPI')
    parser.add_argument("from", choices=Args.from_choices, help=Args.from_desc, type=str.upper)
    parser.add_argument("to", choices=Args.to_choices, help=Args.to_desc, type=str.lower)

    parser.add_argument("source", help=Args.source_desc)

    parser.add_argument("dest", help=Args.dest_desc, default=DefaultVal.dest, nargs="?")
    parser.add_argument("--per_limit", default=DefaultVal.per_limit, type=int, help=Args.per_limit_desc)
    parser.add_argument("--max_limit", default=DefaultVal.max_limit, type=int, help=Args.max_limit_desc)
    parser.add_argument("--max_retry", default=DefaultVal.max_retry, type=int, help=Args.retry_desc)
    parser.add_argument("--r_encoding", default=DefaultVal.default_encoding, help=Args.r_encoding_desc)
    parser.add_argument("--w_encoding", default=DefaultVal.default_encoding, help=Args.w_encoding_desc)
    parser.add_argument("--filter", default=None, help=Args.filter_desc)
    parser.add_argument("--expand", default=None, type=int, help=Args.expand_desc)
    parser.add_argument("--qsn", default=None, type=bool, help=Args.qsn_desc)
    parser.add_argument("--query_body", default=DefaultVal.query_body, type=str, help=Args.query_body_desc)
    parser.add_argument("--write_mode", default=DefaultVal.default_file_mode_w, type=str, help=Args.write_mode_desc)
    parser.add_argument("--key_type", default=DefaultVal.default_key_type, type=str.upper, help=Args.key_type_desc)
    return parser.parse_args()


def get_filter(filter_file):
    if not filter_file:
        return None
    with open(filter_file, "r") as f:
        exec(f.read())
    func = locals()["my_filter"]
    return func


async def getter_to_writer(getter, writer):
    with writer as safe_writer:
        async for items in getter:
            if asyncio.iscoroutinefunction(safe_writer.write):
                await safe_writer.write(items)
            else:
                safe_writer.write(items)


def main():
    args = get_arg()
    from_ = getattr(args, "from")

    from_args = list()
    from_kwargs = dict()
    to_args = list()
    to_kwargs = dict()

    if from_ != Args.from_choices[0]:  # not api
        from_args.extend(args.source.split(":"))
    else:
        from_args.extend([args.source])

    from_kwargs["encoding"] = args.r_encoding
    from_kwargs["key_type"] = args.key_type
    if args.query_body:
        try:
            from_kwargs["query_body"] = json.loads(args.query_body)
        except Exception as e:
            raise SyntaxError("--query_body must be json serialized")

    for key in ("per_limit", "max_limit", "max_retry"):
        from_kwargs[key] = getattr(args, key)

    to_kwargs["filter_"] = get_filter(args.filter)
    to_kwargs["encoding"] = args.w_encoding
    to_kwargs["mode"] = args.write_mode
    to_kwargs["key_type"] = args.key_type
    for key in ("max_retry", "expand", "qsn"):
        to_kwargs[key] = getattr(args, key)

    if from_ not in getter_config_map:
        raise ValueError("argument from must be in %s" % (str(Args.from_choices), ))
    getter_config = getter_config_map[from_](*from_args, **from_kwargs)
    getter = ProcessFactory.create_getter(getter_config)

    if args.to == Args.to_choices[4]:
        # es
        indices, doc_type = args.dest.split(":")
        to_args.append(indices)
        to_args.append(doc_type)
    elif args.to in Args.to_choices[5:]:
        # redis, mysql, mongo
        if args.dest == DefaultVal.dest:
            to_args.append(DefaultVal.dest_without_path)
        else:
            to_args.append(args.dest)
    else:
        dest = args.dest + "." + args.to
        to_args.append(dest)

    writer_config = writer_config_map[args.to](*to_args, **to_kwargs)
    writer = ProcessFactory.create_writer(writer_config)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(getter_to_writer(getter, writer))


if __name__ == "__main__":
    main()
