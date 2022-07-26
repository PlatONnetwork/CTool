import json
import os
import sys

import click
import pandas as pd
import rlp
from hexbytes import HexBytes

from platon_utility import transaction_str_to_int, write_csv, \
    chain_id, decodeBech32Address, encodeAddress, url, unsigned_transaction_file_dir, hrp_type

from client_sdk_python import Web3
from client_sdk_python.eth import Eth
from client_sdk_python.providers import HTTPProvider


# 生成锁仓交易待签名文件
def restrict_unsigned(web3, from_address, nonce, chain_id,
                      restrict_account, restrict_plan) -> dict:
    platon = Eth(web3)
    transaction_dict = {
        "rawTransaction": "",
        'from': from_address,
        # 'from_before_free_amount': platon.getBalance(from_address),
        'from_before_free_amount': web3.fromWei(platon.getBalance(from_address),'ether'),
        "to": encodeAddress("0x1000000000000000000000000000000000000001"),
        "gasPrice": 1000000000,
        "gas": 21000,
        "nonce": nonce,
        "chainId": chain_id,
        "value": "0",
        "restrict_account": restrict_account,
        "restrict_plan": restrict_plan,
        "data": "",
    }
    return transaction_dict


# 生成锁仓交易data
def gen_restrict_data(unsign_data, platon) -> dict:
    plan_list = []
    restrict_plan = unsign_data["restrict_plan"]
    # print(restrict_plan)

    # restrict_plan = json.loads(restrict_plan.replace("'", "\""))

    restrict_account = decodeBech32Address(unsign_data["restrict_account"])

    for dict_ in restrict_plan:
        v = [dict_[k] for k in dict_]
        plan_list.append(v)
    rlp_list = rlp.encode(plan_list)
    data = rlp.encode([rlp.encode(int(4000)),
                       rlp.encode(bytes.fromhex(restrict_account)),
                       rlp_list])
    # 预估gas
    unsign_data["gas"] = platon.estimateGas({
        'from': unsign_data["from"],
        "to": unsign_data["to"],
        "nonce": unsign_data["nonce"],
        "data": data,
        "value": unsign_data["value"],
    })

    return HexBytes(data).hex(), unsign_data["gas"]


@click.command(help='生成锁仓交易待签名文件')
@click.option('-f', '--filepath', 'filePath', required=True, help='锁仓分配文件路径.')
def batch_unsigned_restrict_tx(filePath):
    try:
        if not os.path.exists(filePath):
            print("文件不存在：{}".format(filePath))
            sys.exit(1)

        if not filePath.endswith(".xls") and not filePath.endswith(".xlsx"):
            print("不是execl文件，请检查：{}".format(filePath))
            sys.exit(1)

        # 读取文件路径
        res_data = pd.read_excel(filePath)

        # 保存所有锁仓交易
        all_transactions = []
        # 相同锁仓释放到账账户地址合并保存
        dict_transactions_by_restrict_account = {}

        # 相同from的nonce
        dict_from_to_nonce = {}

        w3 = Web3(HTTPProvider(url), hrp_type=hrp_type)
        platon = Eth(w3)

        for index, row in res_data.iterrows():
            # 锁仓地址
            from_address = row["from"]
            # 锁仓释放到账账户
            release_account = row["release_account"]
            # 锁仓高度(单位为：天, 一天8个结算周期)
            restrict_epoch = int(row["epoch"]) * 8
            # 锁仓金额
            restrict_amount = w3.toWei(str(row["amount"]), "ether")

            # 当前锁仓字典
            restrict_dict = {
                'Epoch': int(restrict_epoch),
                "Amount": restrict_amount,
            }

            # 当前锁仓释放钱包(地址)已存在，进行合并,超过36个重新创建
            dict_key = from_address + "_" + release_account
            if dict_transactions_by_restrict_account.get(dict_key):
                one_transaction_data = dict_transactions_by_restrict_account.get(dict_key)

                restrict_plan = one_transaction_data.get("restrict_plan")
                res_len = len(restrict_plan)
                if res_len >= 36:
                    # 超过36组，需要创建新锁仓计划
                    restrict_plan = [restrict_dict]
                    nonce = dict_from_to_nonce.get(from_address)
                    # 下一笔交易nonce
                    nonce += 1
                    one_transaction_data = restrict_unsigned(w3, from_address, nonce, chain_id,
                                                             release_account, restrict_plan)
                    # 新增锁仓释放钱包(地址)与交易的映射关系
                    dict_transactions_by_restrict_account[dict_key] = one_transaction_data
                    all_transactions.append(one_transaction_data)
                    dict_from_to_nonce[from_address] = nonce
                else:
                    restrict_plan.append(restrict_dict)
            else:
                if from_address in dict_from_to_nonce:
                    nonce = dict_from_to_nonce.get(from_address)
                    nonce = nonce + 1
                else:
                    nonce = platon.getTransactionCount(from_address)
                dict_from_to_nonce[from_address] = nonce
                # 当前锁仓释放钱包(地址)不存在，创建新锁仓计划
                restrict_plan = [restrict_dict]
                one_transaction_data = restrict_unsigned(w3, from_address, nonce, chain_id,
                                                         release_account, restrict_plan)
                # 新增锁仓地址和释放钱包(地址)与交易的映射关系
                dict_transactions_by_restrict_account[dict_key] = one_transaction_data
                all_transactions.append(one_transaction_data)

        # 生成锁仓计划data和预估gas
        transaction_list = []
        for one_transaction in all_transactions:
            transaction_list.append(transaction_str_to_int(one_transaction))

        all_transactions.clear()
        for one_transaction in transaction_list:
            data, gas = gen_restrict_data(one_transaction, platon)
            one_transaction["data"] = data
            one_transaction["gas"] = gas
            one_transaction["restrict_plan"] = json.dumps(one_transaction["restrict_plan"])

            all_transactions.append(one_transaction)

        # 生成 csv 文件
        unsigned_file_csv_name = "unsigned_restrict_transaction.csv"

        unsigned_file_path = os.path.join(unsigned_transaction_file_dir, unsigned_file_csv_name)
        write_csv(unsigned_file_path, all_transactions)

    except Exception as e:
        print('{} {}'.format('exception: ', e))
        print('generate unsigned restrict transaction file failure!!!')
        sys.exit(1)

    else:
        print('{}{} {}'.format('SUCCESS\n', "generate unsigned restrict transaction file:", unsigned_file_path))


if __name__ == "__main__":
    batch_unsigned_restrict_tx()
