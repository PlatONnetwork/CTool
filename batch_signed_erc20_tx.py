import os
import click
import sys

from client_sdk_python.packages.platon_keys.utils.bech32 import decode, encode
from platon_utility import read_csv, \
    signed_transaction_file_dir, sign_one_transaction_by_prikey, \
    transaction_str_to_int, write_csv, get_dir_by_name, get_private_key_from_wallet_file, get_password_file


def translation_address(org_address, tag_hrp_type) -> str:
    """
    地址转换
    :param org_address: 源地址
    :param tag_hrp_type: 目标地址格式前缀
    :return:
    """

    if org_address == "":
        return ""
    org_hrp = org_address[0:3]
    _, address = decode(org_hrp, org_address)
    if address is None:
        raise Exception("bech32Address is invaild:{}".format(org_address))

    tag_address = encode(tag_hrp_type, address)
    if tag_address is None:
        raise Exception("encode address to bech32 is failed！hrp:{}, address:{}".format(tag_hrp_type, address))

    return tag_address


@click.command(help='生成合约交易签名文件')
@click.option('-f', '--filepath', 'filePath', required=True, help='合约交易待签名文件路径.')
@click.option('-c', '--convert_hrp', 'convert_hrp', default='', help='转换地址格式前缀.')
@click.option('-k', '--keystore', required=True, help='签名交易钱包文件所在路径.')
def batch_signed_erc20_tx(filePath, keystore, convert_hrp):
    try:
        if not os.path.exists(filePath):
            print("文件不存在：{}".format(filePath))
            sys.exit(1)

        if '' != convert_hrp and 3 != len(convert_hrp):
            print("指定转换地址前缀错误：{}".format(convert_hrp))
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
        contractName = ""
        funcName = ""
        if len(transaction_list) > 0:
            contractName = transaction_list[0]["contractName"]

        for one_transaction in transaction_list:
            funcName = one_transaction["funcName"]
            # 读取待签名文件中对应from地址的私钥
            # 保存钱包密码的from地址
            save_pass_from = one_transaction["from"]
            org_hrp = save_pass_from[0:3]
            if '' != convert_hrp and org_hrp != convert_hrp:
                save_pass_from = translation_address(save_pass_from, convert_hrp)

            file_name = save_pass_from + ".json"
            find, wallet_dir_path = get_dir_by_name(keystore, file_name)
            if not find:
                print('{} {} {}'.format('The wallet file of', file_name, 'could not be found'))
                sys.exit(1)

            if dict_wallet_address_to_password.get(save_pass_from):
                password = dict_wallet_address_to_password.get(save_pass_from)
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
                password = dict_wallet_address_to_password.get(save_pass_from)

            wallet_file_path = os.path.join(wallet_dir_path, file_name)
            private_key = get_private_key_from_wallet_file(wallet_file_path, password)
            rawTransaction, attrDict = sign_one_transaction_by_prikey(one_transaction, private_key)

            raw_len = len(rawTransaction)
            one_transaction["rawTransaction_remain"] = ""
            max_len = 30000
            if raw_len > max_len:
                rawTransaction_remain = rawTransaction[max_len:raw_len]
                # raw_len = len(rawTransaction_remain)
                rawTransaction = rawTransaction[0:max_len]
                # raw_len = len(rawTransaction)
                one_transaction["rawTransaction_remain"] = rawTransaction_remain

            one_transaction["rawTransaction"] = rawTransaction
            one_transaction["local_hash"] = attrDict.hash.hex()
            sign_transaction_list.append(one_transaction)
            i = i + 1
            print("完成第: %d 笔交易签名=================" % i)

        # 生成 csv 文件
        signed_file_csv_name = "signed_{}_{}_transactions.csv".format(contractName, funcName)
        signed_file_path = os.path.join(signed_transaction_file_dir, signed_file_csv_name)
        write_csv(signed_file_path, sign_transaction_list)

    except Exception as e:
        print('{} {}'.format('exception: ', e))
        print('generate signed erc20 {} {} transaction file failure!!!'.format(contractName, funcName))
        sys.exit(1)

    else:
        print('SUCCESS\nGenerate erc20 {} {} transaction file: {}'.format(contractName, funcName, signed_file_path))


if __name__ == "__main__":
    batch_signed_erc20_tx()
