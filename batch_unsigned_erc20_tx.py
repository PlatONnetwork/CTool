# _*_ coding:utf-8 _*_
import json
import os
import sys
import click
import pandas as pd
from client_sdk_python import HTTPProvider, Web3
from client_sdk_python.eth import Eth
from platon_utility import unsigned_transaction_file_dir, get_abi_func_inputs_by_name, \
    write_csv, url, unsigned_soldity_transaction, hrp_type


# 获取交易函数的data
def get_contract_data(contract, abi_info, params_info):
    func_name = params_info["name"]
    func_params = {}
    param_list = []

    findIndexName = "name"
    # 构造函数
    if "constructor" == func_name:
        findIndexName = "type"

    for interface in abi_info:
        # 函数
        if findIndexName in interface and func_name == interface[findIndexName]:
            func_params = interface
            break

    if 0 == len(func_params):
        raise Exception("Function name {} is not found in the ABI, Please check!".format(func_name))
    # 有函数判断是否有构造函数参数配置文件
    if "inputs" in func_params and len(func_params["inputs"]) > 0:
        len_inputs_param = len(func_params["inputs"])
        param_list = params_info["params"]
        len_param = len(param_list)

        if len_inputs_param != len_param:
            print("构造函数参数个数不匹配：{} 个, 实际需要：{} 个".format(len_param, len_inputs_param))
            sys.exit(1)

    # 构造函数
    if "constructor" == func_name:
        data = contract._encode_constructor_data(args=param_list)
    else:
        data = contract.encodeABI(fn_name=func_name, args=param_list)
    return data


@click.command(help='生成合约交易待签名文件')
@click.option('-f', '--filepath', 'filePath', required=True, help='合约交易文件.')
@click.option('-a', '--abi_path', required=True, help='合约abi所在文件路径.')
@click.option('-b', '--bin_path', required=True, help='合约bin所在文件路径.')
@click.option('-c', '--contract_name', 'contractName', required=True, help='合约名称.')
@click.option('-d', '--contract_address', 'toAddress', required=False, default="", help='合约地址.')
@click.option('-n', '--function_name', 'func_name', required=True, default="transfer", help='函数名.')
def batch_unsigned_erc20_tx(filePath, abi_path, bin_path, contractName,
                            toAddress, func_name):
    try:

        if not os.path.exists(filePath):
            print("合约交易文件不存在：{}".format(filePath))
            sys.exit(1)

        if not filePath.endswith(".xls") and not filePath.endswith(".xlsx"):
            print("不是execl文件，请检查：{}".format(filePath))
            sys.exit(1)

        # abi和bin文件路径
        if not os.path.exists(abi_path) or not os.path.exists(bin_path):
            print("erc20合约的abi或bin文件路径不存在：{},{}".format(abi_path, bin_path))
            sys.exit(1)

        w3 = Web3(HTTPProvider(url), hrp_type=hrp_type)
        platon = Eth(w3)

        erc20_abi = json.load(open(abi_path))
        with open(bin_path) as f:
            erc20_bytecode = f.readlines()

        # 加载abi
        abi_info_list = erc20_abi
        abi_info = []
        findIndexName = "name"
        # 构造函数
        if "constructor" == func_name:
            findIndexName = "type"
        # 过滤匿名函数
        for info in abi_info_list:
            if findIndexName in info:
                abi_info.append(info)

        # 获取合约交易入参名
        func_inputs_list = get_abi_func_inputs_by_name(abi_info, func_name)
        # print(func_inputs_list)
        # 读取合约交易配置文件
        contract_tx_data = pd.read_excel(filePath)
        columns_list = contract_tx_data.columns.values.tolist()
        # 检查字段
        for param in func_inputs_list:
            field_name = param["field_name"]
            if field_name not in columns_list:
                raise Exception("Field name '{}' does not exist in the file: {}, please check!".
                                format(field_name, filePath))

        # 合约对象
        contract = platon.contract(abi=abi_info, bytecode=erc20_bytecode[0])

        dict_from_to_nonce = {}
        all_transaction = []
        i = 0
        for index, row in contract_tx_data.iterrows():
            from_address = row["from"]
            currentToAddress = toAddress
            if "constructor" != func_name and "" == currentToAddress:
                if str(row["contract_address"]) != "nan":
                    currentToAddress = row["contract_address"]
                if "" == currentToAddress:
                    print("调用合约交易: {}, 合约地址不能为空!".format(func_name))
                    sys.exit(1)

            # 获取合约交易参数值
            params = []
            save_params = []
            for param in func_inputs_list:
                field_name = param["field_name"]
                type = param["type"]
                param_value = row[field_name]
                if "uint256" == type:
                    if "transfer" == func_name or "constructor" == func_name:
                        param_value = w3.toWei(str(param_value), "ether")
                    else:
                        param_value = int(param_value)
                params.append(param_value)
                save_params.append(param_value)

            params_info = {
                "name": func_name,
                "params": params
            }

            if dict_from_to_nonce.get(from_address):
                nonce = dict_from_to_nonce.get(from_address)
            else:
                nonce = platon.getTransactionCount(from_address)

            # 组data
            data = get_contract_data(contract, abi_info, params_info)
            # 生成待签名文件
            one_transaction_data = unsigned_soldity_transaction(
                from_address, currentToAddress, data,
                nonce, contractName=contractName, funcName=func_name, func_params=save_params)

            all_transaction.append(one_transaction_data)
            dict_from_to_nonce[from_address] = nonce + 1

            i = i + 1
            print("完成第: %d 笔待签名交易=================" % i)

        # 部署合约
        if "constructor" == func_name:
            func_name = "deploy"
        unsigned_file_csv_name = "unsigned_{}_{}_transactions.csv".format(contractName, func_name)
        unsigned_file_path = os.path.join(unsigned_transaction_file_dir, unsigned_file_csv_name)
        write_csv(unsigned_file_path, all_transaction)

    except Exception as e:
        print('{} {}'.format('exception: ', e))
        print('generate unsigned erc2.0 {} {} transaction file failure!!!'.format(contractName, func_name))
        sys.exit(1)

    else:
        print('SUCCESS\ngenerate unsigned erc2.0 {} {} transaction file: {}'.format(contractName,
                                                                                    func_name,
                                                                                    unsigned_file_path))


if __name__ == "__main__":
    batch_unsigned_erc20_tx()
