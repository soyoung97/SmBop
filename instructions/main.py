import sys
from os import path
sys.path.append(path.dirname( path.dirname( path.abspath(__file__) ) ))
from smbop.utils import moz_sql_parser as msp
from anytree import RenderTree
from smbop.utils import ra_preproc as ra_preproc
from anytree.exporter import DotExporter
from pprint import pprint


class SQLParser():
    def __init__(self):
        self.start = True
        self.idx = 0

    def sql_to_dict(self, sql_string):
        tree_dict = msp.parse(sql_string)
        return tree_dict['query']

    def sql_to_tree(self, sql_string):
        tree_dict = msp.parse(sql_string)
        pprint(tree_dict['query'])
        tree_obj = ra_preproc.ast_to_ra(tree_dict["query"])
        return tree_obj

    """
    def exec(self, sql_string):
        tree_obj = self.sql_to_tree(sql_string)
        print(RenderTree(tree_obj))
        return tree_obj
        """

    def run(self):
        self.get_data()
        for i, x in enumerate(self.data[1550:]):
            print(f"\n\n{i} - Original SQL: {x}\nOriginal tree: ")
            full = parser.sql_to_dict(x)
            pprint(full)
            print(f"\nSplitted: \n")
            res = parser.reconstruct(full)
            print('\n'.join(res))
            self.idx += 1


    def get_data(self):
        global nodeidx
        with open('../dataset/train_gold.sql', 'r') as f:
            data = f.read().strip().split('\n')
        # currently experimenting things without JOIN
        data = [x.split("\t")[0] for x in data if 'JOIN' in x]
        data = [x.replace(";", '') for x in data]
        self.data = data
        return data
        """
        parser = SQLParser()
        for i,d in enumerate(data):
            text = input("-next- : ")
            print(f"Orig:\n{d}")
            tree = parser.exec(d)
            nodeidx = 0
            DotExporter(tree, nodenamefunc=nodenamefunc).to_picture(f"images/{i}.png")
            print(f"Saved as {i}.png")

    def nodenamefunc(node):
        global nodeidx
        nodeidx += 1
        try:
            val = node.val
        except:
            #return f"{nodeidx}\n{node.name}"
            return node.name
        #return f"{nodeidx}\n{node.name}\n{node.val}"
        return f"{node.name}\n{node.val}"
        """

    def _flatten(self, value, aggregate_key=None, aggregate_func=None):
        if type(value) == list:
            if aggregate_key is not None:
                values = []
                for val in value:
                    values.append(self.to_string(val[aggregate_key]))
                value = aggregate_func(values)
            else:
                value = aggregate_func(value)
        else:
            if aggregate_key is not None:
                val = value[aggregate_key]
            else:
                val = value
            value = self.to_string(val)
        return value

    def reconstruct(self, full):
        operations = full.keys()
        res = []
        for op in operations:
            if op == 'select':
                select_columns = self._flatten(full['select'], aggregate_key='value', aggregate_func=lambda x: ','.join(x))
                from_strings = self._flatten(full['from'], aggregate_func=lambda x: '$(PREV)')
                output = f"SELECT {select_columns} FROM {from_strings}"
            elif op == 'from':
                if type(full['from']) == list:
                    output = "SELECT * FROM "
                    join_query = ""
                    for val in full['from']: # TODO: How do we achieve good accuracy on "multiple join on and .." query?
                        if type(val) == str:
                            # TODO: case when it is not T1/T2 but direct column names, we may need to preprocess to unify it
                            output += f"{val} "
                        elif val.get('join') is None: # this item is None
                            output += f"{val['value']} as {val['name']} "
                        else:
                            if type(val['join']) == str:
                                join_query += f"JOIN {val['join']}"
                            else:
                                join_query += f"JOIN {val['join']['value']} as {val['join']['name']} "

                            if val.get('on') is not None:
                                join_query += f"ON {self.to_string(val['on'])}"
                    output += join_query
                else:
                    continue # already processed at select
            elif op == 'where':
                output = f"$(PREV) where {self.to_string(full['where'])}"
            elif op == 'op':
                left = self.reconstruct(full['op']['query1'])
                right = self.reconstruct(full['op']['query2'])
                output = '\n>>left: \n'
                output += '\n'.join(left)
                output += f"\n<<{full['op']['type']}>>"
                output += '\n>>right: \n'
                output += '\n'.join(right)
            elif op == 'groupby':
                if type(full['from']) == list:
                    from_val = "$(PREV)"
                else:
                    from_val = self.to_string(full['from'])
                groupby_columns = self._flatten(full['groupby'], aggregate_key='value', aggregate_func=lambda x: ','.join(x))
                output = f"{from_val} GROUP BY {groupby_columns}"
            elif op == 'having':
                output = f"$(PREV) HAVING {self.to_string(full['having'])}"
            elif op == 'orderby':
                orderby_columns = self._flatten(full['orderby'], aggregate_key='value', aggregate_func=lambda x: ','.join(x))
                output = f"$(PREV) ORDER BY {orderby_columns}"
            elif op == 'limit':
                output = f"$(PREV) LIMIT {self.to_string(full['limit'])}"
            else:
                import pdb; pdb.set_trace()

            res.append(output)
        return res


    def to_string(self, querydict):
        MULTI_FUNC = ['and', 'or']
        SIMPLE_AGG_FUNC = ['distinct', 'count', 'max', 'min', 'avg', 'sum']
        compare2equations = {'add': '+', 'sub': '-', 'eq': '=', 'in': 'IN', 'nin': 'NOT IN', 'neq': '!=', 'gt': '>', 'lt': '<', 'gte': '>=', 'lte': '<='}
        COMPARE_FUNC = compare2equations.keys()
        if type(querydict) in [str, int, float]:
            return querydict
        append_on_end = ''
        keys = list(querydict.keys())
        keys.sort()
        if len(keys) == 1:
            key = keys[0]
        else:
            if keys == ['sort', 'value']:
                sort_value = ' ' + querydict['sort'].upper()
                return f"{self.to_string({'value': querydict['value']})} {sort_value}"
            else:
                import pdb; pdb.set_trace()
        if key in MULTI_FUNC:
            for subquery in querydict[key]:
                return f" {key.upper()} ".join([self.to_string(subquery) for subquery in querydict[key]])
        if key in COMPARE_FUNC:
            left, right = querydict[key]
            left, right = self.to_string(left), self.to_string(right)
            return f"{left} {compare2equations[key]} {right}"
        if key in SIMPLE_AGG_FUNC:
            return f"{key.upper()}({self.to_string(querydict[key])})"
        elif key == 'literal':
            return f"'{querydict['literal']}'"
        elif key == 'value':
            return querydict['value']
        elif key == 'like':
            return f"{querydict['like'][0]} LIKE {querydict['like'][1]}"
        elif key == 'between':
            colname, left, right = querydict[key]
            return f"{colname} BETWEEN {left} AND {right}"
        elif key == 'query':
            print("EDGE CASE")
            print("--------Parsing inner--------")
            print(">>>>> \n".join(self.reconstruct(querydict['query'])))
        elif key == 'missing':
            return f'{querydict['missing']} = "null"'
        else:
            print(self.idx)
            import pdb; pdb.set_trace()




if __name__ == "__main__":
    #query = input("SQL query: ")
    #parser = SQLParser()
    #parser.exec("SELECT creation FROM department GROUP BY creation ORDER BY count(*) DESC LIMIT 1")
    #parser.exec("SELECT DISTINCT Creation FROM (SELECT * FROM (SELECT * FROM department JOIN management  ON department.department_ID = management.department_ID JOIN head ON head.head_ID = management.head_ID) WHERE born_state = 'Alabama')")
    #parser.exec("SELECT DISTINCT T1.creation FROM department AS T1 JOIN management AS T2 ON T1.department_id = T2.department_id JOIN  head AS T3 ON T2.head_id = T3.head_id WHERE T3.born_state = 'Alabama'")
    #parser.exec("SELECT DISTINCT department.creation FROM department JOIN management ON department.department_id = management.department_id JOIN head ON management.head_id = head.head_id WHERE head.born_state = 'Alabama'")
    #debug()

    parser = SQLParser()
    parser.run()
