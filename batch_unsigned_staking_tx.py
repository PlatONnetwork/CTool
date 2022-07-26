import json
import os
import sys
import pandas as pd
import click
import rlp
from client_sdk_python import HTTPProvider, Web3
from client_sdk_python.eth import Eth
from hexbytes import HexBytes
from platon_utility import unsigned_transaction_file_dir, decodeBech32Address, chain_id, \
    write_csv, encodeAddress, url, hrp_type


# 生成质押交易待签名文件
def staking_unsigned(platon, staking_address, staking_amount, amount_type, nonce, validator_info) -> dict:
    # 质押节点信息
    benefitAddress = validator_info["benefitAddress"]
    blsPubKey = validator_info["blsPubKey"]
    nodePublicKey = validator_info["nodePublicKey"]
    externalId = validator_info["externalId"]
    nodeName = validator_info["nodeName"]
    webSite = validator_info["webSite"]
    details = validator_info["details"]
    delegatedRewardRate = validator_info["delegatedRewardRate"]

    # connnect node
    node_url = validator_info['nodeAddress'] + ":" + validator_info['nodeRpcPort']
    w3 = Web3(HTTPProvider(node_url), hrp_type=hrp_type)

    free = w3.platon.getBalance(staking_address)
    free_lat = w3.fromWei(free, "ether")
    # free amount
    if amount_type == 0:
        amount_lat = staking_amount

        if float(free_lat) <= float(amount_lat):
            err_msg = "质押账户余额:{} ATP, 不足于质押:{} ATP".format(free_lat, amount_lat)
            raise Exception(err_msg)
    else:
        if float(free_lat) < 0.1:
            err_msg = "质押账户余额:{} ATP, 不足于质押手续费.".format(free_lat)
            raise Exception(err_msg)

    proInfo = w3.admin.getProgramVersion()
    programVersionSign = proInfo["Sign"]
    programVersion = proInfo["Version"]

    blsProof = w3.admin.getSchnorrNIZKProve()
    benefitAddress = decodeBech32Address(benefitAddress)
    if programVersionSign[:2] == '0x':
        programVersionSign = programVersionSign[2:]
    if blsPubKey[:2] == '0x':
        blsPubKey = blsPubKey[2:]

    # generator tx data
    data = HexBytes(rlp.encode([rlp.encode(int(1000)),
                                rlp.encode(int(amount_type)),
                                rlp.encode(bytes.fromhex(benefitAddress)),
                                rlp.encode(bytes.fromhex(nodePublicKey)),
                                rlp.encode(externalId), rlp.encode(nodeName), rlp.encode(webSite),
                                rlp.encode(details), rlp.encode(w3.toWei(staking_amount, 'ether')),
                                rlp.encode(int(delegatedRewardRate)),
                                rlp.encode(programVersion),
                                rlp.encode(bytes.fromhex(programVersionSign)),
                                rlp.encode(bytes.fromhex(blsPubKey)),
                                rlp.encode(bytes.fromhex(blsProof))])).hex()

    # generator tx info
    to = encodeAddress("0x1000000000000000000000000000000000000002")
    transaction_dict = {
        'from': staking_address,
        'from_before_free_amount': platon.getBalance(staking_address),
        "to": to,
        "nodeid": nodePublicKey,
        "gasPrice": 1000000000,
        "gas": 21000,
        "amount": w3.toWei(staking_amount, "ether"),
        "amount_type": amount_type,
        "free": free,
        "nonce": nonce,
        "chainId": chain_id,
        "additional_info": validator_info,
        "data": data,
    }

    # estimateGas
    transaction_dict["gas"] = w3.eth.estimateGas({
        'from': transaction_dict["from"],
        "to": transaction_dict["to"],
        "data": transaction_dict["data"],
    })

    return transaction_dict


@click.command(help='生成节点质押交易待签名文件')
@click.option('-f', '--filepath', 'filePath', required=True, help='节点质押信息文件路径.')
def batch_unsigned_staking_tx(filePath):
    try:
        if not os.path.exists(filePath):
            print("文件不存在：{}".format(filePath))
            sys.exit(1)

        if not filePath.endswith(".xls") and not filePath.endswith(".xlsx"):
            print("不是execl文件，请检查：{}".format(filePath))
            sys.exit(1)

        # 读取文件路径
        staking_df = pd.read_excel(filePath)

        # 获取转账 to 钱包名称
        all_transaction = []
        w3 = Web3(HTTPProvider(url), hrp_type=hrp_type)
        platon = Eth(w3)
        dict_from_to_nonce = {}

        for index, row in staking_df.iterrows():
            staking_address = row["staking_address"]
            staking_amount = float(row["amount"])
            amount_type = int(row["amount_type"])
            node_info = json.loads(row["node_info"])

            if dict_from_to_nonce.get(staking_address):
                nonce = dict_from_to_nonce.get(staking_address)
            else:
                nonce = platon.getTransactionCount(staking_address)

            one_transaction_data = staking_unsigned(platon, staking_address, staking_amount,
                                                    amount_type, nonce, node_info)
            all_transaction.append(one_transaction_data)
            dict_from_to_nonce[staking_address] = nonce + 1
        # 生成 csv 文件
        unsigned_file_csv_name = "unsigned_staking_transactions.csv"
        unsigned_file_path = os.path.join(unsigned_transaction_file_dir, unsigned_file_csv_name)
        write_csv(unsigned_file_path, all_transaction)

    except Exception as e:
        print('{} {}'.format('exception: ', e))
        print('generate unsigned staking transaction file failure!!!')
        sys.exit(1)

    else:
        print('{}{} {}'.format('SUCCESS\n', "generate unsigned staking transaction file:", unsigned_file_path))


if __name__ == "__main__":
    batch_unsigned_staking_tx()
