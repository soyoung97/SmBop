import sys
from os import path
sys.path.append(path.dirname( path.dirname( path.abspath(__file__) ) ))
from smbop.utils import moz_sql_parser as msp
from anytree import RenderTree
from smbop.utils import ra_preproc as ra_preproc

class SQLParser():
    def __init__(self):
        self.start = True

    def sql_to_tree(self, sql_string):
        tree_dict = msp.parse(sql_string)
        tree_obj = ra_preproc.ast_to_ra(tree_dict["query"])
        return tree_obj

    def exec(self, sql_string):
        tree_obj = self.sql_to_tree(sql_string)
        print(RenderTree(tree_obj))


def debug():
    with open('../dataset/train_gold.sql', 'r') as f:
        data = f.read().strip().split('\n')
    # currently experimenting things without JOIN
    data = [x.split("\t")[0] for x in data if 'JOIN' not in x]
    parser = SQLParser()
    for d in data:
        text = input("-next- : ")
        print(f"Orig:\n{d}")
        parser.exec(d)



if __name__ == "__main__":
    #query = input("SQL query: ")
    #parser = SQLParser()
    #parser.exec("SELECT creation FROM department GROUP BY creation ORDER BY count(*) DESC LIMIT 1")
    #parser.exec("SELECT DISTINCT Creation FROM (SELECT * FROM (SELECT * FROM department JOIN management  ON department.department_ID = management.department_ID JOIN head ON head.head_ID = management.head_ID) WHERE born_state = 'Alabama')")
    #parser.exec("SELECT DISTINCT T1.creation FROM department AS T1 JOIN management AS T2 ON T1.department_id = T2.department_id JOIN  head AS T3 ON T2.head_id = T3.head_id WHERE T3.born_state = 'Alabama'")
    #parser.exec("SELECT DISTINCT department.creation FROM department JOIN management ON department.department_id = management.department_id JOIN head ON management.head_id = head.head_id WHERE head.born_state = 'Alabama'")
    debug()
