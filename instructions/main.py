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

    def sql_to_dict(self, sql_string):
        tree_dict = msp.parse(sql_string)
        return tree_dict['query']

    def sql_to_tree(self, sql_string):
        tree_dict = msp.parse(sql_string)
        pprint(tree_dict['query'])
        tree_obj = ra_preproc.ast_to_ra(tree_dict["query"])
        return tree_obj

    def exec(self, sql_string):
        tree_obj = self.sql_to_tree(sql_string)
        print(RenderTree(tree_obj))
        return tree_obj

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

def get_data():
    global nodeidx
    with open('../dataset/train_gold.sql', 'r') as f:
        data = f.read().strip().split('\n')
    # currently experimenting things without JOIN
    data = [x.split("\t")[0] for x in data] # if 'JOIN' in x]
    data = [x.replace(";", '') for x in data]
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
    """

def debug():
    parser = SQLParser()
    data = get_data()
    for x in data:
        print(f"\n\nOriginal SQL: {x}\nOriginal tree: ")
        full = parser.sql_to_dict(x)
        pprint(full)
        print(f"\nSplitted: \n")
        res = reconstruct(full)
        print('\n'.join(res))


def reconstruct(full):
    operations = full.keys()
    res = []
    for op in operations:
        if op == 'select':
            if type(full['select']) == list:
                values = []

                for val in full['select']:
                    values.append(dict2sql(val['value']))
                value = ','.join(values)
            else:
                value = dict2sql(full['select']['value'])
            if type(full['from']) == list:
                from_val = "$(PREV)"
            else:
                from_val = full['from']
            output = f"SELECT {value} FROM {from_val}"
        elif op == 'from':
            if type(full['from']) == list:
                output = "SELECT * FROM "
                join_query = ""
                for val in full['from']:
                    if val.get('join') is None: # this item is None
                        output += f"{val['value']} as {val['name']} "
                    else:
                        join_query += f"JOIN {val['join']['value']} as {val['join']['name']} "
                        join_query += f"ON {val['on']['eq'][0]} = {val['on']['eq'][1]} "
                output += join_query
            else:
                continue # already processed at select
        elif op == 'where':
            output = f"$(PREV) where {dict2sql(full['where'])}"
        elif op == 'op':
            left = reconstruct(full['op']['query1'])
            right = reconstruct(full['op']['query2'])
            output = '\n>>left: \n'
            output += '\n'.join(left)
            output += f"\n<<{full['op']['type']}>>"
            output += '\n>>right: \n'
            output += '\n'.join(right)
        elif op == 'groupby':
            if type(full['from']) == list:
                from_val = "$(PREV)"
            else:
                from_val = dict2sql(full['from'])
            output = f"{from_val} GROUP BY {dict2sql(full['groupby'])}"
        elif op == 'having':
            output = f"$(PREV) HAVING {dict2sql(full['having'])}"
        elif op == 'orderby':
            output = f"$(PREV) ORDER BY {dict2sql(full['orderby'])}"
        else:
            import pdb; pdb.set_trace()

        res.append(output)
    return res


def dict2sql(querydict):
    if type(querydict) == str:
        return querydict
    if type(querydict) == int:
        return querydict
    keys = querydict.keys()
    if 'eq' in keys:
        left, right = querydict['eq']
        left, right = dict2sql(left), dict2sql(right)
        return f"{left} = {right}"
    elif 'literal' in keys:
        return f"'{querydict['literal']}'"
    elif 'distinct' in keys:
        return f"DISTINCT {querydict['distinct']}"
    elif 'count' in keys:
        return f"COUNT({querydict['count']})"
    elif 'value' in keys:
        return querydict['value']
    elif 'gt' in keys:
        left = dict2sql(querydict['gt'][0])
        right = dict2sql(querydict['gt'][1])
        return f"{left} > {right}"
    elif 'max' in keys:
        return f"MAX({dict2sql(querydict['max'])})"
    elif 'min' in keys:
        return f"MIN({dict2sql(querydict['min'])})"
    elif 'avg' in keys:
        return f"AVG({dict2sql(querydict['avg'])})"
    else:
        import pdb; pdb.set_trace()




if __name__ == "__main__":
    #query = input("SQL query: ")
    #parser = SQLParser()
    #parser.exec("SELECT creation FROM department GROUP BY creation ORDER BY count(*) DESC LIMIT 1")
    #parser.exec("SELECT DISTINCT Creation FROM (SELECT * FROM (SELECT * FROM department JOIN management  ON department.department_ID = management.department_ID JOIN head ON head.head_ID = management.head_ID) WHERE born_state = 'Alabama')")
    #parser.exec("SELECT DISTINCT T1.creation FROM department AS T1 JOIN management AS T2 ON T1.department_id = T2.department_id JOIN  head AS T3 ON T2.head_id = T3.head_id WHERE T3.born_state = 'Alabama'")
    #parser.exec("SELECT DISTINCT department.creation FROM department JOIN management ON department.department_id = management.department_id JOIN head ON management.head_id = head.head_id WHERE head.born_state = 'Alabama'")
    debug()
