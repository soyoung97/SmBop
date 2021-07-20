import sys
from os import path
sys.path.append(path.dirname( path.dirname( path.abspath(__file__) ) ))
from smbop.utils import moz_sql_parser as msp
from anytree import RenderTree
from smbop.utils import ra_preproc as ra_preproc
from anytree.exporter import DotExporter
from pprint import pprint
import re
import json
from collections import defaultdict
from tqdm import tqdm

class SQLParser():
    def __init__(self, verbose=False):
        self.subquery = []
        self.pattern = re.compile(r"\[R_INNER_(\d+)\]")
        self.tok = {'rprev': '[R_PREV]', 'dprev': '[D_PREV]', 'dleft': '[D_LEFT]', 'rleft': '[R_LEFT]', 'dright': '[D_RIGHT]', 'rright': '[R_RIGHT]'}
        self.verbose=verbose

    def sql_to_dict(self, sql_string):
        tree_dict = msp.parse(sql_string)
        return tree_dict['query']

    def sql_to_tree(self, sql_string):
        tree_dict = msp.parse(sql_string)
        pprint(tree_dict['query'])
        tree_obj = ra_preproc.ast_to_ra(tree_dict["query"])
        return tree_obj

    def run(self):
        self.get_data_json()
        unable_split = 0
        different = []
        for i, info in tqdm(enumerate(self.data)):
            x = info['query']
            self.parsed_output = {}
            if self.verbose:
                print(f"\n\n{i} - Original SQL: {x}\nOriginal tree: ")
            try:
                orig_parsed_query = parser.sql_to_dict(x)
            except:
                if self.verbose:
                    print("Was unable to parse corresponding SQL")
                    unable_split += 1
                    self.data[i]['stepwise_query'] = [x]
                    continue
            if self.verbose:
                pprint(orig_parsed_query)
                print(f"\nSplitted: \n")
            parsed_output = parser.reconstruct(orig_parsed_query)
            res = self.order(parsed_output)
            merged = self.merge_subsql(res)
            self.data[i]['stepwise_query'] = res
            if self.verbose:
                print('\n'.join(res))
                print('merged')
                print(merged)
            if merged.upper().replace(' ', '').replace('(', '').replace(')', '') != x.upper().replace(' ', '').replace('(', '').replace(')', ''):
                print("DIFFERNET!!")
                print(x)
                print(merged)
                different.append([x, merged])
            self.subquery = []
        print(f"TOTAL: {len(self.data)}, unable to parse: {unable_split}, {100*unable_split/len(self.data)}%")
        print(f"different: {len(different)}!")
        pprint(different)
        with open('../dataset/train_spider_with_stepwise.json', 'w') as f:
            json.dump(self.data, f, indent=4)
        print("Saved json to [train_spider_with_stepwise.json]!!")

        import pdb; pdb.set_trace()

    def get_data_sql(self):
        with open('../dataset/train_gold.sql', 'r') as f:
            data = f.read().strip().split('\n')
        # currently experimenting things without JOIN
        data = [x.split("\t")[0] for x in data] # if 'LIKE' in x]
        data = [x.replace(";", '').replace('"', "'") for x in data]
        data = [re.sub(' +', ' ', x) for x in data]
        self.data = data
        return data

    def get_data_json(self):
        with open('../dataset/train_spider.json', 'r') as f:
            data = json.load(f)
        for i, info in enumerate(data):
            data[i]['query'] = re.sub(' +', ' ', data[i]['query'].replace(";", '').replace('"', "'"))

        self.data = data
        return data

    def _flatten(self, value, aggregate_func=None):
        if type(value) == list:
            values = []
            for val in value:
                values.append(self.to_string(val))
            value = aggregate_func(values)
        else:
            value = self.to_string(value)
        return value


    def _reconstruct_join(self, from_query): #only when from_query type is list.
        output = f"{self.tok['rprev']} FROM "
        join_query = ""
        for val in from_query: # TODO: How do we achieve good accuracy on "multiple join on and .." query?
            if type(val) == str:
                # TODO: case when it is not T1/T2 but direct column names, we may need to preprocess to unify it
                output += f"{val} "
            elif val.get('join') is None: # this item is None
                output += f"{val['value']} AS {val['name']} "
            else:
                if type(val['join']) == str:
                    join_query += f"JOIN {val['join']} "
                else:
                    join_query += f"JOIN {val['join']['value']} AS {val['join']['name']} "

                if val.get('on') is not None:
                    join_query += f"ON {self.to_string(val['on'])} "
        output += join_query
        return output

    def reconstruct(self, full): # add information into self.parsed_output
        operations = full.keys()
        res = {}
        for op in operations:
            if op == 'select':
                select_columns = self._flatten(full['select'], aggregate_func=lambda x: ', '.join(x))
                from_strings = self.tok['rprev'] if type(full['from']) == list else self.to_string(full['from'])
                output = f"SELECT {select_columns}"
            elif op == 'from':
                if type(full['from']) == list:
                    output = self._reconstruct_join(full['from'])
                else:
                    output = f"{self.tok['rprev']} FROM {self.to_string(full['from'])}" # already processed at select
            elif op == 'where':
                output = f"{self.tok['rprev']} WHERE {self.to_string(full['where'])}"
            elif op == 'op': # in this case, output is not a string but a dictionary
                left = self.reconstruct(full['op']['query1'])
                right = self.reconstruct(full['op']['query2'])
                output = {'left': left, 'right': right, 'op': full['op']['type']}
            elif op == 'groupby':
                groupby_columns = self._flatten(full['groupby'], aggregate_func=lambda x: ', '.join(x))
                output = f"{self.tok['rprev']} GROUP BY {groupby_columns}"
            elif op == 'having':
                output = f"{self.tok['rprev']} HAVING {self.to_string(full['having'])}"
            elif op == 'orderby':
                orderby_columns = self._flatten(full['orderby'], aggregate_func=lambda x: ', '.join(x))
                output = f"{self.tok['rprev']} ORDER BY {orderby_columns}"
            elif op == 'limit':
                output = f"{self.tok['rprev']} LIMIT {self.to_string(full['limit'])}"
            else:
                import pdb; pdb.set_trace()
            res[op] = output
        return res


    def to_string(self, querydict):
        MULTI_FUNC = ['and', 'or']
        SIMPLE_AGG_FUNC = ['distinct', 'count', 'max', 'min', 'avg', 'sum']
        compare2equations = {'add': '+', 'div': '/', 'sub': '-', 'eq': '=', 'in': 'IN', 'nin': 'NOT IN', 'neq': '!=', 'gt': '>', 'lt': '<', 'gte': '>=', 'lte': '<=', 'like': 'LIKE', 'nlike': 'NOT LIKE'}
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
            return self.to_string(querydict['value'])
        elif key == 'between':
            colname, left, right = querydict[key]
            return f"{self.to_string(colname)} BETWEEN {self.to_string(left)} AND {self.to_string(right)}"
        elif key == 'query':
            new_inner_query = self.reconstruct(querydict['query'])
            self.subquery.append(new_inner_query)
            return f"[R_INNER_{len(self.subquery)-1}]"
        elif key == 'missing':
            return f'{querydict["missing"]} = "null"'
        elif key == 'exists':
            return f'{querydict["exists"]} != "null"'
        else:
            import pdb; pdb.set_trace()


    # removes meta information (origin key values corresponding to each subqueries)
    def order(self, rec_q): #reconstructed_queries
        ord_q = []
        if rec_q.get('op') is not None:
            assert len(rec_q.keys()) == 1
            ord_q += [self.tok['dleft'] + ' ' + x for x in self.order(rec_q['op']['left'])]
            ord_q += [self.tok['dright'] + ' ' + x for x in self.order(rec_q['op']['right'])]
            ord_q.append(f"{self.tok['rleft']} {rec_q['op']['op'].upper()} {self.tok['rright']}")
            #if rec_q['op']['left'].get('query') is not None:
            #    import pdb; pdb.set_trace()
            return ord_q

        ORDERING = ['select', 'from', 'where', 'groupby', 'having', 'orderby', 'limit']
        key2ord = {key: i for i, key in enumerate(ORDERING)}
        data = [(key2ord[key], rec_q[key]) for key in rec_q]
        ord_q += [x[1] for x in sorted(data, key=lambda k: k[0])]
        inner_queries = self.find_corresponding_inner_queries(ord_q)
        if inner_queries is not None:
            for num, subq in inner_queries:
                ord_q = [f"[D_INNER_{num}] " + x for x in self.order(subq)] + ord_q
        if self.tok['rprev'] in ord_q[0]:
            print("SHOULD NOT BE Replace to PREV IN HERE!")
            import pdb; pdb.set_trace()
        return ord_q

    def find_corresponding_inner_queries(self, ord_q):
        for query in ord_q:
            if 'R_INNER' in query:
                number = [int(x) for x in self.pattern.findall(query)]
                return [[num, self.subquery[num]] for num in number]
        return None


    def _merge_sql(self, prev, cur):
        pass

    def merge_subsql(self, subsqls):
        res = defaultdict(dict)
        define_pattern = re.compile(r"\[D_([^]]*)\]")
        replace_pattern = re.compile(r"\[R_([^]]*)\]")
        current_main = ''
        # First, group them by sub definition.
        groups = [define_pattern.findall(subsql) for i, subsql in enumerate(subsqls)] # should not include PREV in groups
        # group subsqls by inner group prefixes
        idx = 0
        current_group = None
        group_start = 0
        grouped_subsqls = []
        main_subsqls = []
        while idx != len(subsqls):
            if len(groups[idx]) == 0: #main
                main_subsqls.append(subsqls[idx])
            if current_group is None and len(groups[idx]) != 0: # init
                current_group = groups[idx][0]
                group_start = idx
            else:
                if current_group is not None and (len(groups[idx]) == 0 or groups[idx][0] != current_group):
                    grouped_subsqls.append([current_group, subsqls[group_start:idx]])
                    current_group = None if len(groups[idx]) == 0 else groups[idx][0]
                    group_start = idx
                elif len(groups[idx]) != 0 and idx == (len(subsqls)-1) and current_group == groups[idx][0]:
                    grouped_subsqls.append([current_group, subsqls[group_start:idx+1]])
                # continue for others
            idx += 1
        # merge grouped sqls
        for groupname, grouped_sqls in grouped_subsqls:
            subsubsql = [x.replace(f"[D_{groupname}]", "").strip() for x in grouped_sqls]
            merged_subsubsql = self.merge_subsql(subsubsql)
            res[groupname] = merged_subsubsql.strip()
        # process main sqls
        # fill out inner/left/right/ subqueries at main_subsqls
        filledout_main_subsqls = []
        for subsql in main_subsqls:
            to_replace = replace_pattern.findall(subsql.replace('[R_PREV]', '')) # find patterns to replace except prev mark
            if len(to_replace) == 0:
                filledout_main_subsqls.append(subsql)
            else:
                temp = subsql
                for rep in to_replace:
                    if 'INNER' in rep:
                        temp = temp.replace(f"[R_{rep}]", f"({res[rep].strip()})")
                    else: # should not include brackets here. It is left-right
                        temp = temp.replace(f"[R_{rep}]", f"{res[rep].strip()}")
                filledout_main_subsqls.append(temp)
        main = filledout_main_subsqls[0]
        for subsql in filledout_main_subsqls[1:]:
            main = subsql.replace('[R_PREV]', main)
        # organize answers to single space
        main = re.sub(' +', ' ', main)
        return main


if __name__ == "__main__":
    #query = input("SQL query: ")
    #parser = SQLParser()
    #parser.exec("SELECT creation FROM department GROUP BY creation ORDER BY count(*) DESC LIMIT 1")
    #parser.exec("SELECT DISTINCT Creation FROM (SELECT * FROM (SELECT * FROM department JOIN management  ON department.department_ID = management.department_ID JOIN head ON head.head_ID = management.head_ID) WHERE born_state = 'Alabama')")
    #parser.exec("SELECT DISTINCT T1.creation FROM department AS T1 JOIN management AS T2 ON T1.department_id = T2.deparnt_id JOIN  head AS T3 ON T2.head_id = T3.head_id WHERE T3.born_state = 'Alabama'")
    #parser.exec("SELECT DISTINCT department.creation FROM department JOIN management ON department.department_id = management.department_id JOIN head ON management.head_id = head.head_id WHERE head.born_state = 'Alabama'")
    #debug()

    ex = """[D_INNER_0] [D_LEFT] SELECT StuID FROM Has_allergy as T1 JOIN Allergy_Type as T2 ON T1.Allergy = T2.Allergy
    [D_INNER_0] [D_LEFT] SELECT T1.StuID FROM [R_PREV]
    [D_INNER_0] [D_LEFT] [R_PREV] where T2.allergytype = food
    [D_INNER_0] [D_RIGHT] SELECT * FROM Has_allergy as T1 JOIN Allergy_Type as T2 ON T1.Allergy = T2.Allergy
    [D_INNER_0] [D_RIGHT] SELECT T1.StuID FROM [R_PREV]
    [D_INNER_0] [D_RIGHT] [R_PREV] where T2.allergytype = animal
    [D_INNER_0] [R_LEFT] intersect [R_RIGHT]
    SELECT AVG(age) FROM Student
    [R_PREV] where StuID IN [R_INNER_0]"""
    ex = """[D_INNER_1] [D_INNER_0] SELECT MAX(population) FROM city
    [D_INNER_1] SELECT state_name FROM city
    [D_INNER_1] [R_PREV] where population = [R_INNER_0]
    SELECT river_name FROM river
    [R_PREV] where traverse IN [R_INNER_1]
    [D_INNER_1] [D_INNER_0] SELECT MAX(population) FROM city"""
    ex = [x.strip() for x in ex.split('\n')]
    # SELECT river_name FROM river WHERE traverse IN ( SELECT state_name FROM city WHERE population  =  ( SELECT MAX ( population ) FROM city ) )
    parser = SQLParser()
    parser.run()
