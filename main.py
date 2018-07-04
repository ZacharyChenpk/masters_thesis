import helpers

first_block = 400000
last_block = 400000

#query_string = query.query_database(query.get_block_data(first_block,first_block))

result = helpers.query_database(helpers.get_coinbase(400000,400000))
helpers.write_to_csv(result,'coinbase_400000')

"""
list = df[df.columns[0]].unique()
print(list.shape[0])

#MATCH (b:Block) RETURN avg(apoc.node.degree(b,'CHAINS_TO')) as avg_chain
#MATCH (b:Block) WHERE b.height = 400000 RETURN avg(apoc.node.degree(b,'MINED_IN')) as avg_chain
#MATCH (t:Tx) <-[:MINED_IN]- (b:Block) WHERE b.height=400000 RETURN avg(apoc.node.degree(t,'IN')) as avg_in_t
#MATCH (b:Block) WHERE b.height = 400000 MATCH (b)<-[:MINED_IN]-(t:Tx) RETURN avg(apoc.node.degree(b,'CHAINS_TO')) as avg_IN
#MATCH (b:Block) WHERE b.height = 400000 MATCH (b)<-[:MINED_IN]-(t:Tx) RETURN avg(apoc.node.degree(t,'IN')) as avg_in, avg(apoc.node.degree(t,'OUT')) as avg_out
#MATCH (b:Block {height:400000}) USING INDEX b:Block(height) CALL apoc.path.expandConfig(b,{relationshipFilter:'UNLOCK|SPENT|IN|OUT|LOCK|MINED_IN',bfs:true, uniqueness:"NODE_GLOBAL"}) YIELD path WITH b, LAST(NODES(path)) as a RETURN a LIMIT 50

#COUNT Number of TXs:
# MATCH (b:Block) WHERE b.height = 400000 MATCH (b)<-[:MINED_IN]-(t:Tx) RETURN count(apoc.node.degree(t,'MINED_IN')) as avg_IN

#Depth First Search of Nodes in a given Block excluding other blocks:
#MATCH (b:Block) WHERE b.height = 400000 CALL apoc.path.expandConfig(b,{relationshipFilter:'MINED_IN|UNLOCK|SPENT|IN|OUT|LOCK|',labelFilter:'-Block',bfs:false, uniqueness:"NODE_GLOBAL"}) YIELD path WITH b, LAST(NODES(path)) as a RETURN a LIMIT 100"""


"""
MATCH (b:Block) WHERE b.height = 400000 
CALL apoc.path.expandConfig(b,{relationshipFilter:'MINED_IN|UNLOCK|SPENT|IN|OUT|LOCK|',labelFilter:'-Block',bfs:false, uniqueness:"NODE_GLOBAL"}) 
YIELD path
WITH b, LAST(NODES(path)) as a 
RETURN a LIMIT 100

MATCH (b:Block) where b.height=400000 
CALL apoc.path.subgraphNodes(b, {maxLevel:-1, relationshipFilter:'MINED_IN',labelFilter:'-Block',bfs:true, filterStartNode:true, limit:-1, optional:false}) 
YIELD node  
RETURN node LIMIT 20

MATCH (b:Block) where b.height=400000 
CALL apoc.path.subgraphNodes(b, {labelFilter:'-Block',bfs:false}) YIELD node
RETURN node

MATCH (b:Block) where b.height=400000 
CALL apoc.path.subgraphNodes(b, {labelFilter:'-Block|-File|-Second|-Minute|-Hour|-Day|-Orphan|-TimeTreeRoot|-Year|-Month',bfs:false}) YIELD node
RETURN node LIMIT 25

MATCH (b:Block) where b.height=400000 
CALL apoc.path.subgraphAll(b, {labelFilter:'-Block|-File',bfs:false}) YIELD node
RETURN node LIMIT 20
"""