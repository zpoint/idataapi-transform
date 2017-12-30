
class BaseDataProcess(object):
    @staticmethod
    def expand_dict(origin_item, max_expand=0, current_expand=0, parent_key=None, parent_item=None):
        if max_expand == 0:
            return origin_item
        if max_expand != -1 and current_expand >= max_expand:
            return origin_item
        if parent_key:
            if isinstance(origin_item, dict):
                for sub_k, sub_v in origin_item.items():
                    parent_item[parent_key + "_" + sub_k] = sub_v
                    if parent_key in parent_item:
                        del parent_item[parent_key]
            elif isinstance(origin_item, list):
                for item in origin_item:
                    BaseDataProcess.expand_dict(item, max_expand, current_expand + 1, parent_key, parent_item)
            return origin_item

        keys = [k for k in origin_item.keys()]
        has_sub_dict = False
        for k in keys:
            if isinstance(origin_item[k], dict):
                has_sub_dict = True
                sub_dict = origin_item[k]
                for sub_k, sub_v in sub_dict.items():
                    origin_item[k + "_" + sub_k] = sub_v
                    del origin_item[k]
            elif isinstance(origin_item[k], list):
                for item in origin_item[k]:
                    BaseDataProcess.expand_dict(item, max_expand, current_expand + 1, k, origin_item)

        if has_sub_dict:
            return BaseDataProcess.expand_dict(origin_item, max_expand, current_expand + 1)
        else:
            return origin_item
