import os

import click

from platon_utility import read_csv, \
    signed_transaction_file_dir, sign_one_transaction_by_prikey, \
    transaction_str_to_int, write_csv, get_dir_by_name, get_private_key_from_wallet_file, get_password_file

import sys


@click.command(help='生成节点质押交易签名文件')
@click.option('-f', '--filepath', 'filePath', required=True, help='质押交易待签名文件路径.')
@click.option('-k', '--keystore', required=True, help='签名交易钱包文件所在路径.')
def batch_signed_staking_tx(filePath, keystore):
    try:
        if not os.path.exists(filePath):
            print("文件不存在：{}".format(filePath))
            sys.exit(1)

        # 获取交易列表
        transaction_list_raw = read_csv(filePath)
        transaction_list = []
        for one_transaction in transaction_list_raw:
            transaction_list.append(transaction_str_to_int(one_transaction))

        # 签名交易
        sign_transaction_list = []
        # 钱包地址和钱包密码
        dict_wallet_address_to_password = {}

        i = 0
        for one_transaction in transaction_list:
            # 读取待签名文件中对应from地址的私钥
            file_name = one_transaction["from"] + ".json"
            find, wallet_dir_path = get_dir_by_name(keystore, file_name)
            if not find:
                print('{} {} {}'.format('The wallet file of', file_name, 'could not be found'))
                sys.exit(1)

            if dict_wallet_address_to_password.get(one_transaction["from"]):
                password = dict_wallet_address_to_password.get(one_transaction["from"])
            else:
                # 从密码文件中查找密码
                password_file = get_password_file(wallet_dir_path)
                with open(password_file, 'r') as pf:
                    lines = pf.readlines()
                    for line in lines:
                        address_and_password = line
                        if line.endswith("\n"):
                            address_and_password = address_and_password[:-1]
                        if ':' not in address_and_password:
                            continue
                        info_list = address_and_password.split(":")
                        if 2 != len(info_list):
                            continue
                        address = info_list[0]
                        wallet_password = info_list[1]
                        if 0 == len(address) or 0 == len(wallet_password):
                            continue
                        dict_wallet_address_to_password[address] = wallet_password
                password = dict_wallet_address_to_password.get(one_transaction["from"])

            wallet_file_path = os.path.join(wallet_dir_path, file_name)
            private_key = get_private_key_from_wallet_file(wallet_file_path, password)
            rawTransaction, attrDict = sign_one_transaction_by_prikey(one_transaction, private_key)
            one_transaction["rawTransaction"] = rawTransaction
            one_transaction["local_hash"] = attrDict.hash.hex()
            sign_transaction_list.append(one_transaction)
            i = i + 1
            print("完成第: %d 笔交易签名=================" % i)

        # 生成 csv 文件
        signed_file_csv_name = "signed_staking_transaction.csv"
        signed_file_path = os.path.join(signed_transaction_file_dir, signed_file_csv_name)
        write_csv(signed_file_path, sign_transaction_list)

    except Exception as e:
        print('{} {}'.format('exception: ', e))
        print('generate signed staking transaction file failure!!!')
        sys.exit(1)

    else:
        print('SUCCESS\nGenerate signed staking transaction file: %s' %
              signed_file_path)


if __name__ == "__main__":
    batch_signed_staking_tx()
