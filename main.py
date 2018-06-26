import helper

first_block = 400000
last_block = 400000

#query_string = query.query_database(query.get_block_data(first_block,first_block))

query_string = """MATCH (b:Block {height:400000}) USING INDEX b:Block(height) CALL apoc.path.expandConfig(b,{relationshipFilter:'MINED_IN|UNLOCK|SPENT|IN|OUT|LOCK|',labelFilter:'-Block',bfs:true, uniqueness:"NODE_GLOBAL"}) YIELD path WITH b, LAST(NODES(path)) as a RETURN a LIMIT 10"""

result = helper.query_database(query_string)

helper.write_to_csv(result, 'export3_dsi')



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
#MATCH (b:Block {height:400000}) USING INDEX b:Block(height) CALL apoc.path.expandConfig(b,{relationshipFilter:'MINED_IN|UNLOCK|SPENT|IN|OUT|LOCK|',labelFilter:'-Block',bfs:false, uniqueness:"NODE_GLOBAL"}) YIELD path WITH b, LAST(NODES(path)) as a RETURN a LIMIT 100
"""
