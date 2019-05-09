#
# code that generate the graph (networkx) and other utilities
#

#from enum import Enum
#
#class GraphEdges(Enum):
#    UNIDIR = 1
#    BIDIR  = 2
#    


def create_graph(graph, graph_mapper, nodes_from, edges_from):
    '''
    
    Nodes in the graph will have an attribute '_id_' that was originally the key in the source data.
    
    
    params:
        graph: fully constructed graph object to add new nodes and edges to it.
        graph_mapper:
        node_from:
        edges_from:
        
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

    
    # get list of node types and edge_types
    node_types = graph_mapper['nodes']
    edge_types = graph_mapper['edges']
    
#     print(node_types)
#     print(edge_types)
    
    for node_type in node_types:
        assert all(c in nodes_from.columns for c in node_type['attributes']), \
                "mismatch between nodes_from and mapper's attributes for node: {}".format(node_type['type'])
                
        # do selection of attribute list
        nodes_df = nodes_from.loc[:,node_type['attributes']]
        nodes_df.rename(columns={node_type['key']:'_id_'}, inplace = True)
        # set index on the key
        nodes_df.set_index('_id_', inplace = True)
        nodes_df['_type_'] = node_type['type']
        nodes_list = [x for x in nodes_df.to_dict('index').items()]
        graph.add_nodes_from(nodes_list)

        
    for edge_type in edge_types:
        
        assert all(c in edges_from.columns for c in edge_type['attributes']), \
                "mismatch between edges_from and mapper's attributes for edge: {}".format(edge_type['type'])

        src = edge_type['from']['key']
        src_index = edges_from.columns.get_loc(src)
        dst = edge_type['to']['key']
        dst_index = edges_from.columns.get_loc(dst)

        attr_dict = {}
        for a in edge_type['attributes']:
            attr_dict[a] = edges_from.columns.get_loc(a)
        
        for row in edges_from.itertuples():
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


