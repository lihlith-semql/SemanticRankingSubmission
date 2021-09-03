import json, os, traceback
from graphviz import Digraph
from semql.tree_sampling.sample_trees import ConstraintTreeGenerator
from semql.tree_sampling.table_schema import Graph
from semql.tree_sampling.constraints import Constraints
from semql.execution import SemQLExecutor
from semql.data_sources import BaseDataSource

attribute_triples = json.load(open('../../data/sqllite/formula_1/attribute_triples.json'))
attributes_for_entity_type = json.load(open('../../data/sqllite/formula_1/attributes_for_entity_type.json'))
entities_for_entitytype_attribute_pairs = json.load(
    open('../../data/sqllite/formula_1/entities_for_entitytype_attribute_pairs.json'))
config_dict = json.load(open('../../conf/movie_sampling.json'))

graph = Graph(attribute_triples, attributes_for_entity_type)
cnt = 0

data_name = config_dict['data_name']
BaseDataSource.set('SqliteDataSource', {'db_path': os.path.join( '../../tests/fixtures/sqlite/', '{}.db'.format(data_name))}, key=data_name)

while cnt < 1000:
    try:
        constraint = Constraints(config_dict, graph, entities_for_entitytype_attribute_pairs)
        tree_gen = ConstraintTreeGenerator(constraint)
        tree = tree_gen.generate_tree_from_constraints()

        SemQLExecutor(tree).run()
        tree_dict = tree.to_json('0')

        dot = Digraph(comment='Tree')
        for node_dict in tree_dict:
            dot.node(node_dict['node_id'], label=node_dict['label'])

            for child_id in node_dict['children'].keys():
                dot.edge(node_dict['node_id'], child_id)

        #print(dot.source)
        dot.render('test_output/tree.{}.gv'.format(cnt), view=False)
        ofile = open('test_output/tree.{}.json'.format(cnt), 'wt', encoding='utf-8')
        json.dump(tree_dict, ofile)
        ofile.close()
        cnt += 1
    except Exception as e:
        traceback.print_exc()

