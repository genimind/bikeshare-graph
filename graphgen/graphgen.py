#
# code that generate the graph (networkx) and other utilities
#

import pandas as pd

#from enum import Enum
#
#class GraphEdges(Enum):
#    UNIDIR = 1
#    BIDIR  = 2
#    


def create_graph(graph, graph_mapper, data_provider):
    '''
    
    Nodes in the graph will have an attribute '_id_' that was originally the key in the source data.
    
    
    params:
        graph: fully constructed graph object to add new nodes and edges to it.
        graph_mapper: dictionary describing the type of object to extract
        data_provider: dataframe where attributes and graph objects are created from
        
    return:
        constructured "graph_type" graph object based on the provided source data and according to 
        the mapper schema description.
    '''
#    # graph object
#    gObj = None
#
#    if graph_edges == GraphEdges.BIDIR:
#        gObj = nx.MultiDiGraph()
#    else:
#        gObj = nx.MultiGraph()
#        
    assert (graph != None),"Graph object wasn't constructed correctly"
    assert (isinstance(data_provider, pd.DataFrame)),"The data provider should be a pandas DataFrame"
    
    # get list of node types and edge_types
    node_types = []
    edge_types = []

    if 'nodes' in graph_mapper.keys():
        node_types = graph_mapper['nodes']
    if 'edges' in graph_mapper.keys():
        edge_types = graph_mapper['edges']

    raw_data = data_provider
    
#     print(node_types)
#     print(edge_types)
    
    for node_type in node_types:
        assert all(c in raw_data.columns for c in node_type['attributes']), \
                "mismatch between data_provider and mapper's attributes for node: {} \n \
                    columns: {} - attributes: {}".format(node_type['type'], raw_data.columns, 
                    node_types['attributes'])
                
        # do selection of attribute list
        nodes_df = raw_data.loc[:,node_type['attributes']]
        nodes_df.rename(columns={node_type['key']:'_id_'}, inplace = True)
        # set index on the key
        nodes_df.set_index('_id_', inplace = True)
        nodes_df['_type_'] = node_type['type']
        nodes_list = [x for x in nodes_df.to_dict('index').items()]
        graph.add_nodes_from(nodes_list)

        
    for edge_type in edge_types:
        
        assert all(c in raw_data.columns for c in edge_type['attributes']), \
                "mismatch between data_provider and mapper's attributes for edge: {}".format(edge_type['type'])

        src = edge_type['from']['key']
        src_index = raw_data.columns.get_loc(src)
        dst = edge_type['to']['key']
        dst_index = raw_data.columns.get_loc(dst)

        attr_dict = {}
        for a in edge_type['attributes']:
            attr_dict[a] = raw_data.columns.get_loc(a)
        
        for row in raw_data.itertuples(index = False):
            attr = dict()
            attr['_type_'] = edge_type['type']
            for k,v in attr_dict.items():
                attr[k] = row[v]
            graph.add_edges_from([(row[src_index], row[dst_index], attr)])
        
        
    return graph


def fix_columns(df):
    '''
    '''

    if any(' ' in x for x in df.columns):
        col_rename = dict()
        for c in df.columns:
            col_rename[c] = c.replace(' ', '_')
        df.rename(columns=col_rename, inplace = True)
#     print(df.columns)


