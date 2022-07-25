import os
import random
import sys
import time
import datetime
import click
from client_sdk_python import Web3
from client_sdk_python.eth import Eth
from client_sdk_python.providers import HTTPProvider
from hexbytes import HexBytes
from platon_utility import read_csv, url, write_csv, transaction_result_dir, hrp_type


# 发送交易
def send_transaction(platon, signdata, to_address, waitTx) -> tuple:
    tx_hash = HexBytes(platon.sendRawTransaction(signdata)).hex()
    address = ""
    if waitTx:
        res = platon.waitForTransactionReceipt(tx_hash)
        if to_address is None or to_address == "":
            address = res.contractAddress
    return tx_hash, address


@click.command(help="发送签名交易，包括转账分配/锁仓分配/质押/erc20合约")
@click.option('-f', '--filePath', 'filePath', required=True, help='交易签名文件路径.')
@click.option('-t', '--tx_type', 'tx_type', required=True, default="transfer",
              help='交易类型：transfer/restrict/staking/erc20.',
              type=click.Choice(['transfer', 'restrict', 'staking', 'erc20']))
@click.option('-s', '--sleep_time', 'sleepTime', default=100, help='休眠时间,单位:ms.')
@click.option('-n', '--min_time', 'minTime', default=0, help='最小休眠时间,单位:分钟.')
@click.option('-m', '--max_time', 'maxTime', default=0, help='最大休眠时间,单位:分钟.')
@click.option('--check/--no-check', default=False, help='检查交易是否全部上链.')
@click.option('--wait/--no-wait', default=True, help='是否等待交易回执.')
def batch_send_raw_transaction(filePath, tx_type, sleepTime, check, wait, minTime, maxTime):
    try:
        if minTime > maxTime:
            print("最小休眠时间：{} > 最大休眠时间：{}, 请检查!".format(minTime, maxTime))
            sys.exit(1)
        # 获取 url
        w3 = Web3(HTTPProvider(url), hrp_type=hrp_type)
        platon = Eth(w3)

        # 获取交易列表
        transaction_list_raw = read_csv(filePath)
        tx_count = len(transaction_list_raw)
        if tx_count == 0:
            print('No send transaction!!!')
            return

        # 检查交易是否全部上链
        if check:
            dict_from_to_nonce = {}
            # 取每个账户最大的nonce
            for one_transaction in transaction_list_raw:
                fromAddress = one_transaction["from"]
                nonceSave = int(one_transaction["nonce"])

                if dict_from_to_nonce.get(fromAddress) and nonceSave <= dict_from_to_nonce[fromAddress]:
                    continue
                else:
                    dict_from_to_nonce[fromAddress] = nonceSave

            # 开始检查
            for fromAddress, nonceSave in dict_from_to_nonce.items():
                nonce = platon.getTransactionCount(fromAddress)
                if nonce <= nonceSave:
                    print('not all transactions are on the chain, please wait!!!')
                    return

            print('all transactions are on the chain.')
            return

        # 发送交易
        txResultList = []
        index = 0
        funcName = ""

        if sleepTime > 0:
            sleepTime = float(sleepTime / 1000)

        startTime = datetime.datetime.now()
        tx_len = len(transaction_list_raw)
        # 转账交易/锁仓交易
        for one_transaction in transaction_list_raw:
            transaction_info = {
                "txhash": "",
                "from": one_transaction["from"],
                "to": one_transaction["to"],
                "local_hash": one_transaction["local_hash"],
            }

            if 'from_before_free_amount' in one_transaction:
                transaction_info["from_before_free_amount"] = one_transaction["from_before_free_amount"]
            # 转账交易
            if tx_type == "transfer":
                if 'to_before_free_amount' in one_transaction:
                    transaction_info["to_before_free_amount"] = one_transaction["to_before_free_amount"]
            elif tx_type == "restrict":
                # 锁仓交易:释放到账账户/锁仓计划
                transaction_info["restrict_account"] = one_transaction["restrict_account"]
                transaction_info["restrict_plan"] = one_transaction["restrict_plan"]
            elif tx_type == "staking":
                # 质押交易
                transaction_info["nodeid"] = one_transaction["nodeid"]
            else:
                # erc20合约
                transaction_info["contractName"] = one_transaction["contractName"]
                transaction_info["funcName"] = one_transaction["funcName"]
                transaction_info["funcParams"] = one_transaction["funcParams"]

            if "rawTransaction_remain" in one_transaction and \
                    len(one_transaction["rawTransaction_remain"]) > 0:
                one_transaction["rawTransaction"] = one_transaction["rawTransaction"] \
                                                    + one_transaction["rawTransaction_remain"]
            # 合并字段
            try:
                index = index + 1
                endTime = datetime.datetime.now()
                print("newTime:【{}】, start:【{}】, (newTime-startTime):【{}】".format(endTime,
                                                                                  startTime, endTime - startTime))
                startTime = endTime
                txhash, address = send_transaction(platon, one_transaction["rawTransaction"],
                                                   one_transaction["to"], wait)

                # 随机休眠时间(分钟)
                if maxTime > 0:
                    sleepTime = random.randint(minTime, maxTime) * 60
                    print("sleepTime:{} 秒".format(sleepTime))

                result = "succeed"
                transaction_info["txhash"] = txhash
                transaction_info["result"] = result

                msg = "index:{}, 交易类型：{}, txhash:{}, result:{}".format(index, tx_type, txhash, result)
                # 合约交易
                if 'erc20' == tx_type:
                    funcName = one_transaction["funcName"]
                    contractName = transaction_info["contractName"]
                    if "constructor" == funcName:
                        funcName = "deploy"
                    msg = "{}, contractName:{}, function_name: {}".format(msg, contractName, funcName)
                    if one_transaction["to"] is None or one_transaction["to"] == "":
                        transaction_info["contract_address"] = address
                        msg = "{}, contract_address: {}".format(msg, address)

                print(msg)
                txResultList.append(transaction_info)

                # 休眠(最后一笔交易不休眠)
                if tx_len > index:
                    time.sleep(sleepTime)
            except Exception as e:
                transaction_info["txhash"] = transaction_info["local_hash"]
                transaction_info["result"] = e

                msg = "nonce:{}, index:{}, 交易类型：{}, txhash:{}, result:{}".format(
                    one_transaction["nonce"], index, tx_type, transaction_info["local_hash"], e)

                if 'erc20' == tx_type:
                    funcName = one_transaction["funcName"]
                    contractName = transaction_info["contractName"]
                    if "constructor" == funcName:
                        funcName = "deploy"
                    msg = "{}, contractName:{}, function_name: {}".format(msg, contractName, funcName)
                    if one_transaction["to"] is None or one_transaction["to"] == "":
                        transaction_info["contract_address"] = ""

                print(msg)
                txResultList.append(transaction_info)

        # 写入结果文件
        result_file_csv_name = "{}_transaction_result.csv".format(tx_type)
        if "erc20" == tx_type:
            result_file_csv_name = "{}_{}_transaction_result.csv".format(contractName, funcName)
        result_file_path = os.path.join(transaction_result_dir, result_file_csv_name)
        write_csv(result_file_path, txResultList)
    except Exception as e:
        print('{} {}'.format('exception: ', e))
        print('batch send {} transaction failure!!!'.format(tx_type))
        sys.exit(1)
    else:
        print('batch send transaction SUCCESS!\n')
        print("generate send {} transaction result file: {}".format(tx_type, result_file_path))


if __name__ == "__main__":
    batch_send_raw_transaction()
