import argparse
import requests
import json
import time
import pprint
import pandas as pd

bc_printer = pprint.PrettyPrinter(indent=3)
class ApiEndpoint:
    """Base Blockchain Ingestion Class

    ApiEndpoint provides an interface to define common functions 
    expected by the Breadth First Search of the blockchain
    exploration code.

    Attributes:
        base: Base string URL of the API endpoint
        address: String appended to the end of base to retrieve
            address data.
        transact: String appended to the end of the base to
            retrieve transactional data.
    """

    def __init__(self):
        """Inits Class attributes

        Sets the member  strings which are combined to form
        requests to the API endpoint. Subclasses must
        overload the init function.
        """
        self.base = None
        self.address = None
        self.transact = None

    def getBase(self):
        """Returns the base API URL
        
        Returns: A string of the base API URL
        """
        return self.base

    def getAddress(self, addr):
        """Retrieves data for an address

        Polls the API Addr Endpoint and returns the JSON
        object received from the API.

        Args:
            addr: Hash of the address object in the blockchain.

        Returns: A JSON object with the data on addr
        """
        pass
    
    def getTransaction(self, trans):
        """Retrieves data for a transaction
        
        Polls the API Transaction Endpoint and returns the
        JSON object received from the API.

        Args:
            trans: Hash of the transaction object in the 
                blockchain.

        Returns: A JSON object with the data on trans
        """
        pass

    def addrError(self, code, addr):
        """Prints error message while retreiving an Address

        Structures an error message to report problems while
        requesting data on an address. The message reports
        the status code of the request and the address
        causing the error.

        Args:
            code: HTTP Status Code from the request
            addr: Address hash used in the API request

        Returns: A None value for use in later functions so
            that errors can be elegantly resolved.
        """
        print("Error[", code, "] - Address was: ", addr)
        return None

    def transError(self, code, trans):
        """Prints error message while retreiving a transaction

        Structures an error message to report problems while
        requesting data on a transaction. The message reports
        the status code of the request and the transaction
        causing the error.

        Args:
            code: HTTP Status Code from the request
            addr: Transaction hash used in the API request

        Returns: A None value for use in later functions so
            that errors can be elegantly resolved.
        """
        print("Error[", code, "] - Transction was: ", trans)
        return None

class Blockcypher(ApiEndpoint):
    def __init__(self):
        self.base = "https://api.blockcypher.com/v1/btc/main"
        self.address = "/addrs/"
        self.transact = "/txs/"

    def getAddress(self, addr, full = False):
        api_call = self.base+self.address+addr
        if full:
            api_call = api_call + "/full?txlimit=50"
        try:
            response = requests.get(api_call)
        except requests.exceptions.SSLError as e:
            print("[getAddress] SSL Cert Error")
            print(e)
            return None

        print("Getting address: ", api_call)
        time.sleep(2)
        if response.status_code == 200:
            return response.json()
        else:
            return super().addrError(response.status_code, addr)

    def getTransaction(self, trans):
        response = requests.get(self.base+self.transact+trans)
        time.sleep(2)
        if response.status_code == 200:
            return response.json()
        else:
            return super().transError(response.status_code, trans)

class Blockstream(ApiEndpoint):
    def __init__(self):
        self.base = "https://blockstream.info/api"
        self.address = "/address/"
        self.transact = "/tx/"

    def getAddress(self, addr):
        response = requests.get(self.base+self.address+addr)
        time.sleep(2)
        if response.status_code == 200:
            return response.json()
        else:
            return super().addrError(response.status_code, addr)

    def getTransaction(self, trans):
        response = reequests.get(self.base+self.transact+trans)
        time.sleep(2)
        if response.status_code == 200:
            return response.json()
        else:
            return super().transError(response.status_code, trans)

def nextAddresses(next_url, next_key, key):
    """Retrieve remaining inputs or outputs from endpoint.

    The API is limited to returning a maximum number of addresses in
    inputs and outputs. The function makes additional requests to
    grab the remaining addresses in the list. The Blockcypher
    transaction object will have a property (either next_inputs or
    next_outputs) with the URL to poll the next batch of data. The
    URL uses the parameters instart and outstart to control the 
    offset of the input and output lists, respectively.

    Note that a request to a next_url will also have a next_url
    property even if there are no more addresses. As a result,
    we have to check for an empty list.

    Args:
        next_url: API URL to get next batch of addresses
        next_key:
            Property name used to retrieve the URL for the
            following batch of addresses.
        key: String used to access either inputs or outputs
            of the transaction

    Returns: Dictionary with the full list of inputs and
        outputs for a transaction as well as the amount of
        crypto currency each address contributed to the
        transaction.
    """
    addresses = {}
    while next_url:
        print("      Getting Next URL: ", next_url)
        response = requests.get(next_url)
        time.sleep(2)

        # If the status code is not 200, we have likely run into our request limit
        # for the hour. We cease unnecessary requests and return the addresses we
        # have managed to collect to exit gracefully.
        if response.status_code != 200:
            print("Error in nextAddresses - ", response.status_code, ". url: ", next_url)
            print(response.text)
            return addresses

        data = response.json()
        n_addr = getNewAddresses(data, key)
        print("      Length of new addresses: ", len(n_addr))
        addresses.update(n_addr)

        # We check the lenght of n_addr because sometimes a next_url is provided
        # even when there are no more addresses.
        if len(n_addr) != 0 and next_key in data.keys():
            next_url = data[next_key]
        else:
            next_url = None
    return addresses

def getNewAddresses(t_data, key):
    """Collect addresses into a dictionary

    Creates key-value pairs from the addresses and currency
    sent or received by the address. The function uses key
    to generalize between input and output lists.

    Args:
        t_data: Transaction data from the API
        key: Property name containing the pertinent address

    Returns: Dictionary containing the addresses associated
        with the amount of cryptocurrency sent or received
        by the address
    """
    inputs = {}
    for i in t_data[key]:
        # The addresses property is always a list and our code only expects
        # one value in the list. This check serves as a warning if this 
        # assumption is violated.
        if not i:
            print("[getNewAddresses] i is None")
            bc_printer.pprint(t_data)
            continue
        if len(i["addresses"]) > 1:
            print("Transaction ", t_data["hash"], " has more than one address.")
        for addr in i["addresses"][:5]:
            if key == "inputs":
                inputs[addr] = i["output_value"]
            else:
                inputs[addr] = i["value"]
    return inputs

def expandTransaction(t_data):
    """Extracts neighboring addresses from transaction

    Extracts the input and output addresses connected to
    the given transaction. The data may not contain a 
    complete list of addresses so the function makes
    subsequent calls to retrieve the complete list.

    Args:
        t_data: Transaction JSON object

    Returns: Dictionary with the connected addresses
        organized into inputs and outputs. The
        timestamp of the transaction is also included.
    """
    
    neighbors = set([])
    print("    Getting Inputs")
    inputs = getNewAddresses(t_data, "inputs")
    neighbors = neighbors.union(inputs.keys())
    # if "next_inputs" in t_data.keys():
        # print("    Input getting additional addresses")
        # inputs.update(nextAddresses(t_data["next_inputs"], "next_inputs", "inputs"))

    print("    Getting Outputs")
    outputs = getNewAddresses(t_data, "outputs")
    neighbors = neighbors.union(outputs.keys())
    # if "next_outputs" in t_data.keys():
        # print("    Output getting additional addresses")
        # outputs.update(nextAddresses(t_data["next_outputs"], "next_outputs", "outputs"))

    interactions = { "inputs": inputs,
            "outputs": outputs,
            "timestamp": t_data["received"]}
    return interactions, neighbors

def getNeighbors(block_api, address, transactions):
    """Retrieves an address's neighboring addresses

    Iterates through the transactions associated with the
    given address and collects all the neighboring
    addresses. Transactions may form cycles so we keep
    track of previous transactions in the variable
    named transactions.

    Args:
        block_api: ApiEndpoint subclass handling
            interactions with the API
        address: String hash of address to expand
        transactions: Python set containing all
            previously traversed transaction hashes

    Returns:
        transactions: Python set containing all
            transaction hashes traversed previously
            and in this function execution
        neighbors: Python set of addresses exposed
            by the transactions traversed
        neighbor_links: Dictionary of transactions
            and organized input and out addresses
    """

    # We use the full Address endpoint to reduce the number of requests
    # made per run. However, doing so reduces the number of transactions
    # available in a single call.
    data = block_api.getAddress(address, full=True)
    if not data:
        print("No data for address: ", address)
        return transactions, set([]), None
    neighbors = set([])
    neighbor_links = {}
    for trans in data["txs"]:
        if trans["hash"] not in transactions:
            print("  Expanding transaction: ", trans["hash"])
            transactions.add(trans["hash"])
            t_data = trans #block_api.getTransaction(trans['tx_hash'])
            print("  Addresses involved in transaction: ", len(t_data["addresses"]))
            t, n = expandTransaction(t_data)
            neighbors = neighbors.union(n)
            neighbor_links[trans["hash"]] = t
    return transactions, neighbors, neighbor_links

def getNetwork(block_api, address, jumps):
    """Get addresses N hops away

    Uses Breadth First Search to find all nodes N hops away from
    the given address. All newly discovered addresses form a new
    layer of nodes to expand. Cycles exist in the data so we
    track addresses and transactions visited to prevent infinite
    loops.

    Args:
        block_api: ApiEndpoint subclass used to make API requests
        address: Starting address at the center of the network
        jumps: Number of hops to expand away from the address

    Returns: Dictionary of addresses and transactions found by
        the algorithm.
    """

    network = {}
    trans = set([])
    current_layer = set([address])
    for i in range(jumps):
        print("Starting jump: ", i)
        next_layer = set([])
        for addr in current_layer:
            trans, neighbors, links = getNeighbors(block_api, addr, trans)
            network[addr] = links
            # Keys to the network are the address hashes of all nodes we've
            # visited.  We only want to add nodes to next_layer which we
            # haven't visited. We will explore next_layer in the next jump.
            next_layer = next_layer.union(neighbors.difference(network.keys()))
        current_layer = next_layer
    return network

def writeData(data):
    """Writes input and output CSVs

    Separates the network dictionary into two tables and writes
    them to CSVs. The dictionary can be written to JSON but
    Cypher would not have an easy time ingesting the data so
    CSVs provide a cleaner format. The function breaks down the
    dictionary into a table of transactions and their input
    nodes and a table of transactions and their output nodes.

    Args:
        data: Dictionary containing the explored addresses and
            transactions
    """

    inputs = []
    outputs = []
    for i in data:
        for trans in data[i]:
            for in_node in data[i][trans]["inputs"]:
                in_record = { "input_node": in_node,
                            "amount": data[i][trans]["inputs"][in_node],
                            "trans": trans,
                            "timestamp":data[i][trans]["timestamp"]
                            }
                inputs.append(in_record)
            for out_node in data[i][trans]["outputs"]:
                out_record = { "trans": trans,
                            "amount": data[i][trans]["outputs"][out_node],
                            "output_node": out_node,
                            "timestamp":data[i][trans]["timestamp"]
                            }
                outputs.append(out_record)
    df = pd.DataFrame.from_dict(inputs)
    df.to_csv("input_nodes.csv", index = False)
    df = pd.DataFrame.from_dict(outputs)
    df.to_csv("output_nodes.csv", index = False)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    api_group = parser.add_mutually_exclusive_group(required=True)
    api_group.add_argument("-bc", "--blockcypher", action = "store_true")
    api_group.add_argument("-bs", "--blockstream", action = "store_true")
    parser.add_argument("address", help = "Extract information for specified address")
    parser.add_argument("-n", "--hops", default = 3, type = int, help = "Number of steps away from address")
    args = parser.parse_args()

    if args.blockcypher:
        block_api = Blockcypher()
    elif args.blockstream:
        block_api = Blockstream()

    data = getNetwork(block_api, args.address, args.hops)
    writeData(data)