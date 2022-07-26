import json
import os
import sys

import click
import pandas as pd
import rlp
from hexbytes import HexBytes

from platon_utility import url, handleTxResult, write_csv, read_csv, get_time_stamp, GetRestrictingInfo, \
    encodeAddress, transaction_result_dir, hrp_type

from client_sdk_python import Web3
from client_sdk_python.eth import Eth
from client_sdk_python.providers import HTTPProvider


# 返回转账分配的配置
def get_transfer_retry_tx(df, errList):
    retry_tx_list = []
    for errDict in errList:
        to_address = errDict["to"]

        # 根据to地址查找
        info_tmp = df[(df["to"] == to_address)].copy()
        retry_tx_list.append(info_tmp)

    # 连接表数据
    retry_info = pd.concat(retry_tx_list)

    return retry_info


# 返回质押的配置
def get_staking_retry_tx(df, errList):
    staking_retry_list = []
    for errDict in errList:
        nodeid = errDict["nodeid"]

        # 根据nodeid查找
        staking_df = df[(df["nodeid"] == nodeid)].copy()
        staking_retry_list.append(staking_df)

    # 连接表数据
    retry_info = pd.concat(staking_retry_list)

    return retry_info


# 返回erc20的配置
def get_erc20_retry_tx(df, errList):
    erc20_retry_list = []
    for errDict in errList:
        err_index = errDict["index"]
        erc20_df = df.loc[err_index:err_index].copy()
        erc20_retry_list.append(erc20_df)

    # 连接表数据
    retry_info = pd.concat(erc20_retry_list)

    return retry_info


# 返回锁仓分配的配置
def get_restrict_retry_tx(df, errList):
    all_retry_tx_list = []
    for errDict in errList:
        account_retry_list = []
        from_address = errDict["from"]
        restrict_account = errDict["restrict_account"]
        restrict_plans = json.loads(errDict["restrict_plan"])

        res_len = len(restrict_plans)
        # 根据to地址查找
        acc_df = df[(df["release_account"] == restrict_account) &
                    (df["from"] == from_address)].copy()
        acc_len = len(acc_df)

        if res_len == acc_len:
            # 合并记录全错
            account_retry_list.append(acc_df)
        elif acc_len > res_len:
            # 记录剩余的记录为错误交易
            account_restrict_infos = acc_df[acc_len - res_len:acc_len].copy()
            account_retry_list.append(account_restrict_infos)
        else:
            raise Exception("check restrict transaction by signed file error!")
            sys.exit(1)

        all_retry_tx_list = all_retry_tx_list + account_retry_list

    # 连接表数据
    retry_info = pd.concat(all_retry_tx_list)

    return retry_info


# 生成需要重发交易的分配文件
def gen_retry_alloc_file_by_err_list(filePath, errList, retryPath, tx_type, funcName=""):
    df = pd.read_excel(filePath)

    if tx_type == "transfer":
        # 转账分配
        new_df = get_transfer_retry_tx(df, errList)
    elif tx_type == "restrict":
        new_df = get_restrict_retry_tx(df, errList)
    elif tx_type == "staking":
        new_df = get_staking_retry_tx(df, errList)
    elif tx_type == "erc20":
        new_df = get_erc20_retry_tx(df, errList)

    gen_retry_file(retryPath, new_df, tx_type, funcName)
    print('生成需要重新发送【{}】交易的分配文件路径:{}'.format(tx_type, retryPath))


# 生成分配文件
def gen_retry_file(filePath, new_df, tx_type, funcName=""):
    writer = pd.ExcelWriter(filePath, engine='xlsxwriter')
    # 设置格式
    workbook = writer.book
    worksheets = writer.sheets
    num_format = workbook.add_format({'num_format': '#,##0.00000000'})
    integer = workbook.add_format({'num_format': '#,##0'})
    if tx_type == "transfer":
        # 转账分配
        sheet_name = '转账'
        new_df.to_excel(writer, sheet_name=sheet_name, index=False, merge_cells=False)
        # 设置特定单元格的宽度
        worksheets[sheet_name].set_column(0, 0, 45)
        worksheets[sheet_name].set_column(1, 1, 45)
        worksheets[sheet_name].set_column(2, 2, 15, num_format)
    elif tx_type == "restrict":
        sheet_name = '锁仓'
        new_df.to_excel(writer, sheet_name=sheet_name, index=False, merge_cells=False)
        worksheets[sheet_name].set_column(0, 0, 45)
        worksheets[sheet_name].set_column(1, 1, 45)
        worksheets[sheet_name].set_column(2, 2, 15, integer)
        worksheets[sheet_name].set_column(3, 3, 15, num_format)
    elif tx_type == "staking":
        sheet_name = '批量质押'
        new_df.to_excel(writer, sheet_name=sheet_name, index=None, merge_cells=False)
        worksheets[sheet_name].set_column(0, 0, 15)
        worksheets[sheet_name].set_column(1, 1, 60)
        worksheets[sheet_name].set_column(2, 2, 45)
        worksheets[sheet_name].set_column(3, 3, 15)
        worksheets[sheet_name].set_column(4, 4, 15)
        worksheets[sheet_name].set_column(5, 5, 30)
    elif tx_type == "erc20":
        sheet_name = funcName
        new_df.to_excel(writer, sheet_name=sheet_name, index=None, merge_cells=False)
        worksheets[sheet_name].set_column(0, 0, 45)
        worksheets[sheet_name].set_column(1, 1, 45)
        worksheets[sheet_name].set_column(2, 2, 20)
        worksheets[sheet_name].set_column(3, 3, 20)

    writer.save()
    writer.close()


# 校验转账交易
def check_transfer_tx_result(web3, sign_file):
    tx_res_list = read_csv(sign_file)
    tx_count = len(tx_res_list)
    if tx_count == 0:
        print('No transaction need to check!!!')
        return [], []

    platon = Eth(web3)
    # 检查交易列表
    check_res_list = []
    # 异常交易列表
    err_list = []
    index = 0

    # 检查交易是否全部上链
    dict_from_to_nonce = {}
    dict_from_to_amount = {}
    for tx_res in tx_res_list:
        index = index + 1
        # 转账到账账户地址
        to_address = tx_res["to"]
        # 转账金额
        value = int(tx_res["value"])

        from_address = tx_res["from"]
        res_info = {
            "index": index,
            "from": from_address,
            "from_before_free_amount": 0,
            "from_after_free_amount": 0,
            "to": to_address,
            'to_before_free_amount': 0,
            'to_after_free_amount': web3.fromWei(platon.getBalance(to_address), "ether"),
            "amount": web3.fromWei(value, "ether"),
            # "amount": value,
            "result": "交易成功.",
        }

        if 'from_before_free_amount' in tx_res:
            res_info["from_before_free_amount"] = tx_res["from_before_free_amount"]

        if 'to_before_free_amount' in tx_res:
            res_info["to_before_free_amount"] = tx_res["to_before_free_amount"]

        if dict_from_to_nonce.get(from_address):
            nonce = dict_from_to_nonce.get(from_address)
            from_after_free_amount = dict_from_to_amount.get(from_address)
        else:
            # from_after_free_amount = platon.getBalance(from_address)
            from_after_free_amount = web3.fromWei(platon.getBalance(from_address), "ether")
            nonce = platon.getTransactionCount(from_address)
            dict_from_to_nonce[from_address] = nonce
            dict_from_to_amount[from_address] = from_after_free_amount

        res_info["from_after_free_amount"] = from_after_free_amount

        nonceSave = int(tx_res["nonce"])
        if nonceSave >= nonce:
            res_info["result"] = "交易失败!"
            err_list.append(res_info)

        check_res_list.append(res_info)

        print("index:{}, from:{}, to:{}, from_free_amount:{}, to_free_amount:{}, result:{}".
              format(index, res_info["from"], res_info["to"], res_info["from_after_free_amount"],
                     res_info["to_after_free_amount"], res_info["result"]))

    return check_res_list, err_list


# 校验锁仓交易
def check_restrict_tx_result(web3, sign_file):
    tx_res_list = read_csv(sign_file)
    tx_count = len(tx_res_list)
    if tx_count == 0:
        print('No transaction need to check!!!')
        return [], []

    platon = Eth(web3)
    # 检查交易列表
    check_res_list = []
    # 异常交易列表
    err_list = []
    # 遍历表
    index = 0

    for tx_res in tx_res_list:
        index = index + 1
        res_info = {
            "index": index,
            "from": tx_res["from"],
            "from_before_free_amount": 0,
            "from_after_free_amount": web3.fromWei(platon.getBalance(tx_res["from"]), "ether"),
            "to": tx_res["to"],
            "restrict_account": tx_res["restrict_account"],
            "restrict_plan": tx_res["restrict_plan"],
            "get_restrict_amount": 0,
            "restrict_amount_in_sign_file": 0,
            "result": "交易成功.",
        }

        if 'from_before_free_amount' in tx_res:
            res_info["to_before_free_amount"] = tx_res["from_before_free_amount"]

        res_plans = json.loads(tx_res["restrict_plan"])
        res_amount_in_file = 0
        for plan in res_plans:
            res_amount_in_file = res_amount_in_file + int(plan["Amount"])
        # 获取锁仓信息
        res_plan, res_amount = GetRestrictingInfo(platon, tx_res["from"], tx_res["restrict_account"])
        res_info["get_restrict_amount"] = res_amount
        res_info["restrict_amount_in_sign_file"] = res_amount_in_file
        if res_plan is None or res_amount < res_amount_in_file:
            res_info["result"] = "交易失败!"
            err_list.append(res_info)
        check_res_list.append(res_info)

        print("index:{}, from:{}, restrict_account:{},"
              "from_before_free_amount:{}, from_after_free_amount:{}, result:{}".
              format(index, res_info["from"], res_info["restrict_account"], res_info["from_before_free_amount"],
                     res_info["from_after_free_amount"], res_info["result"]))

    return check_res_list, err_list


# 查询质押信息(1105)
def getStakingInfo(platon, nodeId):
    if nodeId[:2] == '0x':
        nodeId = nodeId[2:]
    data = HexBytes(rlp.encode([rlp.encode(int(1105)),
                                rlp.encode(bytes.fromhex(nodeId))])).hex()

    staking_info = ""
    to = encodeAddress("0x1000000000000000000000000000000000000002")
    try:
        recive = platon.call({"data": data, "to": to})
        recive = str(recive, encoding="ISO-8859-1")
        recive = json.loads(recive)
        code = int(recive['Code'])
        if code != 0:
            return ""
        # print("查询质押信息成功,质押信息:{}".format(recive))
        staking_info = recive["Ret"]
    except Exception as e:
        print("查询质押信息失败,error message:{}".format(e))

    return staking_info


# 校验质押交易
def check_staking_tx_result(web3, sign_file):
    tx_res_list = read_csv(sign_file)
    tx_count = len(tx_res_list)
    if tx_count == 0:
        print('No transaction need to check!!!')
        return [], []

    platon = Eth(web3)
    # 检查交易列表
    check_res_list = []
    # 异常交易列表
    err_list = []
    # 遍历表
    index = 0

    for tx_res in tx_res_list:
        index = index + 1
        res_info = {
            "index": index,
            "from": tx_res["from"],
            "from_before_free_amount": 0,
            "from_after_free_amount": web3.fromWei(platon.getBalance(tx_res["from"]), "ether"),
            "to": tx_res["to"],
            "nodeid": tx_res["nodeid"],
            "result": "交易成功.",
        }

        if 'from_before_free_amount' in tx_res:
            res_info["from_before_free_amount"] = tx_res["from_before_free_amount"]

        staking_info = getStakingInfo(platon, tx_res["nodeid"])
        if staking_info == "":
            res_info["result"] = "交易失败!"
            err_list.append(res_info)
        check_res_list.append(res_info)

    return check_res_list, err_list


# 生成校验结果
def gen_check_result_by_sign_file(web3, sign_file, tx_type):
    # 检查交易列表
    check_res_list = []
    # 异常交易列表
    err_list = []

    if tx_type == "transfer":
        check_res_list, err_list = check_transfer_tx_result(web3, sign_file)
    elif tx_type == "restrict":
        check_res_list, err_list = check_restrict_tx_result(web3, sign_file)
    elif tx_type == "staking":
        check_res_list, err_list = check_staking_tx_result(web3, sign_file)
    else:
        print("unknow transaction type：{} !".format(tx_type))

    return check_res_list, err_list


@click.command(help="校验转账，锁仓分配, 质押，合约等交易结果")
@click.option('-f', '--filepath', 'filePath', required=True, help='交易分配文件路径.')
@click.option('-r', '--resultfile', 'resFilePath', required=True,
              help='交易结果文件/交易签名文件,没有交易文件使用交易签名进行验证.')
@click.option('-t', '--tx_type', 'tx_type', required=True, default="transfer",
              help='交易类型：transfer/restrict/staking/erc20.',
              type=click.Choice(['transfer', 'restrict', 'staking', 'erc20']))
@click.option('--check_by_hash/--no-check_by_hash', default=True, help='是否通过交易hahs校验.')
def verify_transaction_result(filePath, resFilePath, tx_type, check_by_hash):
    try:
        if not os.path.exists(filePath):
            print("交易分配文件不存在：{}".format(filePath))
            sys.exit(1)

        if not os.path.exists(resFilePath):
            print("交易结果文件/交易签名文件不存在：{}".format(resFilePath))
            sys.exit(0)
        # 获取 url
        w3 = Web3(HTTPProvider(url), hrp_type=hrp_type)
        platon = Eth(w3)

        # 检查交易列表
        check_res_list = []
        # 异常交易列表
        err_list = []
        index = 0
        funcName = ""
        if not check_by_hash:
            check_res_list, err_list = gen_check_result_by_sign_file(w3, resFilePath, tx_type)
        else:
            # 获取交易结果列表
            tx_res_list = read_csv(resFilePath)
            tx_count = len(tx_res_list)
            if tx_count == 0:
                print('No transaction need to check!!!')
                return
            else:
                # 验证能否连上节点
                platon.blockNumber

            if 'erc20' == tx_type:
                funcName = tx_res_list[0]["funcName"]
                if "constructor" == funcName:
                    funcName = "deploy"
            for tx_res in tx_res_list:
                try:
                    # 交易hash
                    txhash = tx_res["txhash"]
                    tx_res["from_after_free_amount"] = \
                        w3.fromWei(platon.getBalance(tx_res["from"]), "ether")

                    tx_res["to_after_free_amount"] = 0
                    if tx_type == "transfer":
                        tx_res["to_after_free_amount"] = \
                            w3.fromWei(platon.getBalance(tx_res["to"]), "ether")

                    result = platon.getTransactionReceipt(txhash)

                    if tx_type == "transfer":
                        # 转账交易
                        result, exitCode = handleTxResult(result, "status")
                    elif tx_type == "restrict" or tx_type == "staking":
                        # 锁仓分配/质押
                        result, exitCode = handleTxResult(result)
                    elif tx_type == "erc20":
                        # erc20交易
                        # result, exitCode = handleTxResult(result, "event")
                        result, exitCode = handleTxResult(result, "status")
                    else:
                        print("unknow transaction type：{} !".format(tx_type))
                        return
                except Exception as e:
                    print("index:{}, check tx:{}, from_free_amount:{}, to_free_amount:{}, result:{}".
                          format(index, txhash, tx_res["from_after_free_amount"],
                                 tx_res["to_after_free_amount"], e))
                    tx_res["result"] = e
                    tx_res["index"] = index
                    check_res_list.append(tx_res)
                    err_list.append(tx_res)
                    index = index + 1
                else:
                    print("index:{}, check tx:{}, from_free_amount:{}, to_free_amount:{}, result:{}".
                          format(index, txhash, tx_res["from_after_free_amount"],
                                 tx_res["to_after_free_amount"], result))
                    tx_res["result"] = result
                    check_res_list.append(tx_res)
                    # 保存错误交易
                    if exitCode != 0:
                        tx_res["index"] = index
                        err_list.append(tx_res)

                    index = index + 1
    except Exception as res_e:
        print('check transaction result failure:{}!!!'.format(res_e))
        sys.exit(1)

    else:
        stamp = get_time_stamp()
        currPath = transaction_result_dir

        if len(check_res_list) > 0:
            print('check transaction result SUCCESS!\n')
            # 生成 csv 文件
            check_result_csv_name = "check_{}_result_{}.csv".format(tx_type, stamp)
            if 'erc20' == tx_type:
                contractName = check_res_list[0]["contractName"]
                check_result_csv_name = "check_{}_{}_result_{}.csv".format(contractName, funcName, stamp)

            checkResPath = os.path.join(currPath, check_result_csv_name)
            print("generate check transaction result file: ", checkResPath)
            write_csv(checkResPath, check_res_list)

        # 保存错误记录
        if len(err_list) > 0:
            err_result_csv_name = "err_{}_result_{}.csv".format(tx_type, stamp)
            if 'erc20' == tx_type:
                contractName = err_list[0]["contractName"]
                err_result_csv_name = "err_{}_{}_result_{}.csv".format(contractName, funcName, stamp)
            errResPath = os.path.join(currPath, err_result_csv_name)
            print("generate error transaction result file: ", errResPath)
            write_csv(errResPath, err_list)

            # 生成需要重发交易的分配文件
            retryName = "{}_file_{}.xlsx".format(tx_type, stamp)
            if 'erc20' == tx_type:
                contractName = err_list[0]["contractName"]
                retryName = "{}_{}_file_{}.xlsx".format(contractName, funcName, stamp)
            retryPath = os.path.join(currPath, retryName)
            gen_retry_alloc_file_by_err_list(filePath, err_list, retryPath, tx_type, funcName)


if __name__ == "__main__":
    verify_transaction_result()
