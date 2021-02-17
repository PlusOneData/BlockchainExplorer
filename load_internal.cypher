LOAD CSV WITH HEADERS FROM "file:///internal_trans.csv" AS line
MERGE (pt:Transaction:ExternalTransaction {hash:line.parent})
MERGE (i:Address {address:line.input})
MERGE (o:Address {address:line.output})
MERGE (it:Transaction:InternalTransaction {hash:line.hash})
ON CREATE SET it.value = line.value,
	it.timestamp = datetime(line.confirmed),
    it.gas = line.gas,
    it.gas_price = line.gas_price
MERGE (pt)-[:CALLS]->(it)
MERGE (i)-[:INPUT_TO]->(it)-[:OUTPUT_OF]->(o)
RETURN i, o, it, pt