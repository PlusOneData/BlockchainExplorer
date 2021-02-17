import requests
import argparse
import pprint
import time

import pandas as pd

from btc_explorer import ApiEndpoint

eth_printer = pprint.PrettyPrinter(indent=3)

class EthBlockcypher(ApiEndpoint):
    def __init__(self):
        self.base = "https://api.blockcypher.com/v1/eth/main/"
        self.address = "addrs/"
        self.transact = "txs/"
    
    def getAddress(self, addr, total_trans, full = False):
        target_url = self.base + self.address + addr
        data = self.getResponse(target_url, "Address")
        
        transactions = set([])
        if not data:
            print("No data for address: ", addr)
            return transactions
        
        passes = 0
        for trans in data["txrefs"]:
            if passes == 5:
                break
            if trans["tx_output_n"] == -1 and trans["tx_hash"] not in total_trans:
                transactions.add(trans["tx_hash"])
                passes += 1

        return transactions
        
    def getTransaction(self, trans, total_trans, internal = False, 
            exp_internal = True):
        target_url = self.base + self.transact + trans
        err_msg = "Internal Transaction" if internal else "Transaction"
        data = self.getResponse(target_url, err_msg)
        
        links = []
        i_links = []
        if not data:
            print("No data for transaction: ", trans)
            return links, i_links, total_trans
        
        if internal:
            interaction = EthBlockcypher.populateEvent(data, internal)
            i_links.append(interaction)
            if "internal_txs" in data:
                print("[getTransaction] THERE ARE INTERNAL TXS IN INTERNAL TXS!!!")
        elif trans != data["hash"] and "parent_tx" in data:
            interaction = EthBlockcypher.populateEvent(data, True)
            interaction["hash"] = trans
            i_links.append(interaction)
            if data["parent_tx"] not in total_trans:
                total_trans.add(data["parent_tx"])
                l, i_l, total_trans = self.getTransaction(data["parent_tx"],
                    total_trans,
                    exp_internal = False
                    )
                links.extend(l)
        elif trans!= data["hash"] and "parent_tx" not in data:
            print("Missmatched hash without parent transaction")
            print("[getTransaction] ",trans,"(",data["hash"],")")
        else:
            interaction = EthBlockcypher.populateEvent(data)
            links.append(interaction)
            if "internal_txids" in data and exp_internal:
                for i_t in data["internal_txids"]:
                    if i_t not in total_trans:
                        total_trans.add(i_t)
                        l, i_l, total_trans = self.getTransaction(i_t, 
                            total_trans, 
                            internal = True
                        )
                        i_links.extend(i_l)
        return links, i_links, total_trans

    def getResponse(self, target_url, target):
        try:
            response = requests.get(target_url)
        except requests.exceptions.SSLError as e:
            print("[getResponse] SSL Cert Error")
            print(e)
            return None
            
        time.sleep(1)
        if response.status_code == 200:
            return response.json()
        else:
            print("Error[", response.status_code, "] - ", 
                target, " was: ", eth_printer.pformat(response)
            )
            return None
            
    def populateEvent(data, internal = False):
        event = {
                "input": data["inputs"][0]["addresses"][0],
                "hash": data["hash"],
                "confirmed": data["confirmed"],
                "value": data["total"],
                "gas": data["gas_used"],
                "gas_price": data["gas_price"],
                "output": data["outputs"][0]["addresses"][0]
        }
        if internal:
            event["parent"] = data["parent_tx"]
            if "script" in data["outputs"][0]:
                print("[populateEvent] Internal Transaction has output script")
                print("[populateEvent] tx_hash - "+event["hash"])
                print("[populateEvent] script - "+data["script"])
        else:
            o = data["outputs"][0]
            if "script" in o:
                s = EthBlockcypher.parseScript(o["script"])
                if s:
                    event["func_type"] = s["type"]
                    event["func_addr"] = s["addr"][2:]
                    event["func_val"]  = s["val"]
        return event
        
    def parseScript(script):
        f_code = script[:8]
        if f_code == "a9059cbb":
            a = script[8:72]
            v = script[72:]
            r = {}
            r["type"] = "transfer"
            r["addr"] = hex(int("0x"+a, 16))
            r["val"] = int("0x"+v, 16)
            return r
        elif f_code == "":
            return r
        else:
            return None
            
class EthereumScan(ApiEndpoint):
    def __init__(self, apikey):
        self.base = "https://api.etherscan.io/api"
        self.apikey = apikey
        
    def getAddress(self, addr, full=False):
        payload = {
            "module": "account",
            "action": "txlist",
            "address": "0x"+addr,
            "startblock": "0",
            "endblock": "99999999",
            "sort": "asc",
            "apikey": self.apikey
        }
        data = getResponse(payload, "Address")
        return data
        
    def getTransaction(self, trans, internal = False):
        payload = {
            "module": "account",
            "action": "txlistinternal",
            "txhash": "0x"+trans,
            "startblock": "0",
            "endblock": "99999999",
            "sort": "asc",
            "apikey": self.apikey            
        }
        data = getResponse(payload, "Transaction")
        return data
        
    def getResponse(self, payload, target):
        try:
            response = requests.get(self.base, payload)
        except ssl.SSLCertVerificationError:
            print("[getResponse] SSL Cert Error")
            return None
            
        time.sleep(1)
        if response.status_code == 200:
            return response.json()
        else:
            print("Error[", response.code, "] - ", target, " was: ", hash)
            return None

def expandAddress(block_api, addr, total_trans):   
    transactions = block_api.getAddress(addr, total_trans)
    total_trans = total_trans.union(transactions)
    return total_trans, transactions

def expandTransaction(block_api, transactions, total_trans):
    links = []
    i_links = []
    neighbors = set([])
    for t in transactions:
        interaction, i_interaction, total_trans = block_api.getTransaction(
            t, 
            total_trans
        )

        links.extend(interaction)
        i_links.extend(i_interaction)
        for i in interaction:
            neighbors.add(i["output"])
            
    return neighbors, links, i_links, total_trans

def getNetwork(block_api, a_hash, max_hops):
    addrs = set([a_hash])
    transactions = set([])
    visited_addrs = set([])
    internal_trans = []
    network = {}
    debug = True
    debug_trans = set([
        "4789b02e4aa5e17c653b08bf124be09dd221c0267b7e60a6760c6895c24b1bb7",
        "1d9fe111b3057a3e5f210743ac3f808fdf43286f4d3271a023f7494b62a4cde6",
        "bea70727c01a40ecbaecd69929b0672d27192c2b340c2627dd843260055fd082"
    ])
    for i in range(max_hops):
        next_layer = set([])
        visited_addrs = visited_addrs.union(addrs)
        print("[getNetwork] Addresses - ")
        eth_printer.pprint(addrs)
        if not addrs:
            break
        for addr in addrs:
            print("[getNetwork] Processing addr - ", addr)
            transactions, trans = expandAddress(block_api, addr, transactions)
            if debug:
                trans = trans.union(debug_trans)
                debug = False
            print("[getNetwork] Trans - ")
            eth_printer.pprint(trans)
            neighbors, links, i_links, transactions = expandTransaction(block_api, 
                trans,
                transactions
                )
            network[addr] = links
            next_layer = next_layer.union(neighbors.difference(network.keys()))
            internal_trans.extend(i_links)
        addrs = next_layer
    return network, internal_trans
    
def writeData(data, i_data):
    t_data = []
    for key in data:
        t_data.extend(data[key])
    df = pd.DataFrame.from_dict(t_data)
    df.to_csv("transactions.csv", index = False)
    df = pd.DataFrame.from_dict(i_data)
    df.to_csv("internal_trans.csv", index = False)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("addr_hash", help = "Address hash to expand")
    parser.add_argument("hops", type = int,
        help = "Max number of hops to expand")
    args = parser.parse_args()
    
    block_api = EthBlockcypher()
    data, i_data = getNetwork(block_api, args.addr_hash, args.hops)
    print("NETWORK")
    eth_printer.pprint(data)
    print("INTERNAL")
    eth_printer.pprint(i_data)
    writeData(data, i_data)