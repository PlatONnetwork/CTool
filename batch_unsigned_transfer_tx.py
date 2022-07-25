import os
import sys

import click
import pandas as pd
from platon_utility import url, write_csv, chain_id, unsigned_transaction_file_dir, hrp_type

from client_sdk_python import Web3
from client_sdk_python.eth import Eth
from client_sdk_python.providers import HTTPProvider


# 生成未签名交易
def transfer_unsign(web3, from_address, to_address, value, nonce) -> dict:
    platon = Eth(web3)
    from_before_free_amount = web3.fromWei(platon.getBalance(from_address), "ether")
    to_before_free_amount = web3.fromWei(platon.getBalance(to_address), "ether")
    transaction_dict = {
        'from': from_address,
        'from_before_free_amount': from_before_free_amount,
        "to": to_address,
        'to_before_free_amount': to_before_free_amount,
        "gasPrice": 1000000000,
        "gas": 4700000,
        "nonce": nonce,
        "data": "",
        "chainId": chain_id,
        "value": value,
    }
    return transaction_dict


@click.command(help="生成普通转账交易待签名文件")
@click.option('-f', '--filepath', 'filePath', required=True, help='转账分配文件.')
def batch_unsigned_transfer_tx(filePath):
    try:
        if not os.path.exists(filePath):
            print("文件不存在：{}".format(filePath))
            sys.exit(1)

        if not filePath.endswith(".xls") and not filePath.endswith(".xlsx"):
            print("不是execl文件，请检查：{}".format(filePath))
            sys.exit(1)

        # 获取转账 to 钱包名称
        all_transaction = []
        w3 = Web3(HTTPProvider(url), hrp_type=hrp_type)
        platon = Eth(w3)

        # 读取转账分配表
        transfer_data = pd.read_excel(filePath)

        dict_from_to_nonce = {}
        i = 0
        for index, row in transfer_data.iterrows():
            from_address = row["from"]

            # 转账到账账户地址
            to_address = row["to"]
            # 转账金额
            transfer_amount = row["value"]
            value = w3.toWei(str(transfer_amount), "ether")

            if dict_from_to_nonce.get(from_address):
                nonce = dict_from_to_nonce.get(from_address)
            else:
                nonce = platon.getTransactionCount(from_address)

            one_transaction_data = transfer_unsign(w3, from_address, to_address, value, nonce)
            all_transaction.append(one_transaction_data)
            dict_from_to_nonce[from_address] = nonce + 1

            i = i + 1
            print("完成第: %d 笔待签名交易=================" % i)

        # 生成 csv 文件
        unsigned_file_csv_name = "unsigned_transfer_transactions.csv"

        unsigned_file_path = os.path.join(unsigned_transaction_file_dir, unsigned_file_csv_name)
        write_csv(unsigned_file_path, all_transaction)

    except Exception as e:
        print('{} {}'.format('exception: ', e))
        print('from:{}, to:{}'.format(from_address, to_address))
        print('generate unsigned transfer transaction file failure!!!')
        sys.exit(1)

    else:
        print('{}{} {}'.format('SUCCESS\n', "generate unsigned transfer transaction file:", unsigned_file_path))


if __name__ == "__main__":
    batch_unsigned_transfer_tx()
