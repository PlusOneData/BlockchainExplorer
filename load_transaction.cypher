LOAD CSV WITH HEADERS FROM "file:///transactions.csv" AS line
MERGE (i:Address {address:line.input})
MERGE (o:Address {address:line.output})
MERGE (t:Transaction:ExternalTransaction {hash:line.hash})
ON CREATE SET t.value = line.value,
	t.gas = line.gas,
    t.gas_price = line.gas_price,
    t.confirmed = DATETIME(line.confirmed)
MERGE (i)-[:INPUT_TO]->(t)-[:OUTPUT_OF]->(o)
FOREACH(f in CASE WHEN line.func_addr IS NOT NULL THEN [1] ELSE [] END |
	MERGE (a:Address {address:SUBSTRING(line.func_addr,2)})
    CREATE (t)-[fun:FUNC_CALL]->(a)
    SET fun.value = toInteger(line.func_val)
)
RETURN i,t,o