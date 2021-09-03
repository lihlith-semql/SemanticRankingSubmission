from typing import List, Dict, Set
import json
from collections import defaultdict
from random import choice


class Node:
    def __init__(self, name: str, attributes: List[str]):
        self.name = name
        self.neighbours = dict()
        self.attributes = attributes

    def add_neighbour(self, node, this_attr, reference_attr):
        self.neighbours[node] = (this_attr, reference_attr)
        node.neighbours[self] = (reference_attr, this_attr)

    def __eq__(self, other):
        return self.name == other.name

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def __hash__(self):
        return hash(self.name)


class Graph:
    def __init__(self, table_to_references: Dict, attributes_for_table: Dict):
        self.nodes = dict()

        for table, attributes in attributes_for_table.items():
            node = Node(table, attributes)
            self.nodes[table] = node

        for table, references in table_to_references.items():
            node = self.nodes[table]
            for ref_node_name, attr_name, ref_attr_name in references:
                ref_node = self.nodes[ref_node_name]
                node.add_neighbour(ref_node, attr_name, ref_attr_name)
        self.entity_nodes = self.build_entity_nodes()

    def build_entity_nodes(self):
        entity_nodes = []
        for node_name, node in self.nodes.items():
            attributes = [x for x in node.attributes if x[2] == 'PRI']
            foreign_attributes = [x for x in node.attributes if x[2] == 'MUL']
            if len(attributes) == 1 and len(foreign_attributes) == 0:
                entity_nodes.append(node)
        return entity_nodes

    def reconstruct_paths(self, path_tuples: Dict[Node, Set[Node]], node1: Node, node2: Node, path: List, visited: Dict, full_paths: List):
        path.append(node2)
        visited[node2] = True

        if node1 == node2:
            full_paths.append([x for x in path])
        else:
            for parent in path_tuples[node2]:
                if not visited[parent]:
                    self.reconstruct_paths(path_tuples, node1, parent, path, visited, full_paths)
        path.pop()
        visited[node2] = False

    def all_fixed_paths(self, start_node: Node, depth: int, visited_nodes: Dict[Node, int]) -> List[List[Node]]:
        """
        Returns all paths of a fixed length starting from a predefined node
        :param start_node: node from which to start
        :depth depth: length of all paths
        :return: list of all paths starting from start_node of depth
        """
        visited_nodes[start_node] = depth
        if depth == 1:
            return [[start_node]]
        else:
            paths = []
            for node in start_node.neighbours.keys():
                if visited_nodes.get(node, 0) > depth:
                    continue
                node_paths = self.all_fixed_paths(node, depth - 1, visited_nodes)
                paths.extend(node_paths)
            for path in paths:
                path.append(start_node)

        return paths

    def shortest_path(self, node1: Node, node2: Node):
        marked_nodes = dict()

        queue = list()
        queue.append((node1, 0))
        marked_nodes[node1] = 0
        parents = defaultdict(lambda: set())
        while len(queue) > 0:
            w, w_level = queue.pop(0)
            if w == node2:
                break
            else:
                for neigh_node, (attr, other_attr) in w.neighbours.items():
                    mark_level = marked_nodes.get(neigh_node, w_level)
                    if mark_level >= w_level:
                        marked_nodes[neigh_node] = w_level + 1
                        parents[neigh_node].add(w)

                        queue.append((neigh_node, w_level + 1))
        paths = []
        self.reconstruct_paths(parents, node1, node2, [], defaultdict(lambda: False), paths)
        return paths

    def sample_path(self, start_node=None, k=5):
        if start_node is None:
            start_node = choice(list(self.nodes.values()))

        path = [start_node]
        for i in range(k - 1):
            if len(start_node.neighbours) == 0:
                return path
            start_node = choice(list(start_node.neighbours.keys()))
            path.append(start_node)
        return path


if __name__ == "__main__":
    table_to_references = json.load(open('../../data/table_to_references.json'))
    attributes_for_table = json.load(open('../../data/attributes_for_table.json'))

    graph = Graph(table_to_references, attributes_for_table)

    node1 = graph.nodes['company']
    node2 = graph.nodes['person']
    paths = graph.shortest_path(node1, node2)
    print(paths)

    for _ in range(100):
        path = graph.sample_path(k=4)
        print(path)

    print('------------------------------------------------')
    for _ in range(100):
        node1 = choice(list(graph.nodes.values()))
        node2 = choice(list(graph.nodes.values()))
        paths = graph.shortest_path(node1, node2)
        print(paths)
    print('------------------------------------------------')
    for node in graph.entity_nodes:
        for d in range(7):
            paths = graph.all_fixed_paths(node, d + 1, defaultdict(lambda: 0))
            print(d + 1, paths)