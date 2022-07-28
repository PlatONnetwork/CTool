import csv
import json
import os
import sys
import time
import uuid
from Crypto import Random
import qrcode
import binascii
import rlp
from client_sdk_python import Web3
import pandas as pd
from client_sdk_python.packages.platon_account import Account
from client_sdk_python.packages.platon_account.datastructures import AttributeDict

from client_sdk_python.packages.platon_keyfile.keyfile import \
    _scrypt_hash, encode_hex_no_prefix, encrypt_aes_ctr
from client_sdk_python.packages.platon_keys.utils.bech32 import decode, encode

from client_sdk_python.packages.platon_keys import keys
from client_sdk_python.packages.eth_utils import (
    big_endian_to_int,
    int_to_big_endian,
    keccak,
)
from client_sdk_python.packages.eth_utils.curried import (
    text_if_str,
    to_bytes,
)

from hexbytes import HexBytes
from copy import copy

# 主网链id
chain_id = 100
# 地址前缀
hrp_type = "lat"

# 获取配置文件信息
file_path = os.path.split(os.path.realpath(__file__))[0]
# 合约目录
contract_path = os.path.join(file_path, "contract")
config_file = os.path.join(file_path, 'config/config.json')
with open(config_file, 'r') as load_f:
    config_info = json.load(load_f)
    # 配置生成钱包目录
    wallet_file_base_dir = config_info["wallet_file_base_dir"]
    # 配置生成钱包私钥目录
    prikey_file_base_dir = config_info["prikey_file_base_dir"]
    validator_file_base_dir = config_info['validator_file_base_dir']
    unsigned_transaction_file_dir = config_info['unsigned_transaction_file_dir']
    signed_transaction_file_dir = config_info['signed_transaction_file_dir']
    # 发送交易结果目录
    transaction_result_dir = config_info['transaction_result_dir']

    # url = config_info["nodeAddress"] + ":" + config_info["nodeRpcPort"]
    url = config_info["rpc_url"]
    chain_id = config_info['chain_id']
    hrp_type = config_info['hrp_type']

    if not os.path.exists(unsigned_transaction_file_dir):
        os.makedirs(unsigned_transaction_file_dir)

    if not os.path.exists(signed_transaction_file_dir):
        os.makedirs(signed_transaction_file_dir)

    if not os.path.exists(transaction_result_dir):
        os.makedirs(transaction_result_dir)

    if not os.path.exists(validator_file_base_dir):
        os.makedirs(validator_file_base_dir)


def get_abi_func_inputs_by_name(abi_info, func_name):
    '''
    :param abi_info: 合约abi
    :param func_name: 合约函数名称
    :return:
    返回跟合约函数参数匹配的字段名称
    '''
    func_inputs_list = []

    findIndexName = "name"
    # 构造函数
    if "constructor" == func_name:
        findIndexName = "type"

    func_params = {}
    for interface in abi_info:
        # 函数
        if findIndexName in interface and func_name == interface[findIndexName]:
            func_params = interface
            break

    # 在abi文件中没有找到对应的函数名
    if 0 == len(func_params):
        raise Exception("Function name {} is not found in the ABI, Please check!".format(func_name))

    if "inputs" in func_params and len(func_params["inputs"]) > 0:

        # 有参数
        for param in func_params["inputs"]:
            field_name = "param" + param["name"]
            input = {
                "field_name": field_name,
                "type": param["type"]
            }
            func_inputs_list.append(input)

    return func_inputs_list


def unsigned_soldity_transaction(from_address, to_address, data, nonce,
                                 gas_price=1000000000, amount='0',
                                 contractName="", funcName="", func_params="") -> dict:
    """
    组装未签名交易格式字典数据
    :return:
        交易字典
    """
    transaction_dict = {
        "rawTransaction": "",
        'from': from_address,
        "to": to_address,
        "gasPrice": gas_price,
        "gas": 4700000,
        "nonce": nonce,
        "chainId": chain_id,
        "value": amount,
        "contractName": contractName,
        "funcName": funcName,
        "funcParams": func_params,
        "data": data,
    }

    return transaction_dict


def unsigned_transaction(from_address, to_address, data, nonce, gas_price, amount, params_info) -> dict:
    """
    组装未签名交易格式字典数据
    :return:
        交易字典
    """
    transaction_dict = {
        "rawTransaction": "",
        'from': from_address,
        "to": to_address,
        "gasPrice": gas_price,
        "gas": 4700000,
        "nonce": nonce,
        "chainId": chain_id,
        "value": amount,
        "data": data,
        "params_info": params_info,
    }

    return transaction_dict


def encodeAddress(_address):
    """
    序列化0x地址为bech32格式的地址字符串
    :return:
        bech32格式地址字符串
    """
    if _address[:2] == '0x':
        _address = _address[2:]

    _address = list(bytes.fromhex(_address))
    ret = encode(hrp_type, _address)
    if ret is None:
        raise Exception("encode address to bech32 is failed!")

    return ret


def decodeBech32Address(_bech32Address):
    if _bech32Address == "":
        return ""
    """
    反序列化bech32格式地址为0x地址
    :return:
        0x地址格式字符串
    """
    _, address = decode(hrp_type, _bech32Address)
    if address is None:
        raise Exception("bech32Address is invaild:{}".format(_bech32Address))

    return bytes(address).hex()


def get_dir_by_name(dir_name, file_name):
    """获取某一目录及其子目录下的某一个名称所在的路径

        Args:
            dir_name: 要搜索的目录路径
            file_name： 要查找文件名称

        Returns:
            bool： 表示是否找到文件
            str:  查找的文件所在的路径
        """
    for _, dir_list, files in os.walk(dir_name):
        for one_file in files:
            if one_file.lower() == file_name.lower():
                return True, dir_name
        for one_dir in dir_list:
            new_dir_name = os.path.join(dir_name, one_dir)
            result, result_file = get_dir_by_name(new_dir_name, file_name)
            if result:
                return True, result_file
    return False, None


# 返回密码文件路径
def get_password_file(wallet_dir_path):
    password_file = ""
    for s in os.listdir(wallet_dir_path):
        if s.startswith("password") and s.endswith(".txt"):
            password_file = os.path.join(wallet_dir_path, s)
            break
    # print("password_file:{}".format(password_file))
    if "" == password_file:
        print('The wallet password file does not exist under {}, please check!'.
              format(wallet_dir_path))
        sys.exit(1)

    return password_file


def get_file_by_name(dir_name, file_name):
    """获取某一目录及其子目录下的某一个名称的文件

    Args:
        dir_name: 要搜索的目录路径
        file_name： 要查找文件名称

    Returns:
        bool： 表示是否找到文件
        str： 查找的文件的完整路径
    """
    for _, dir_list, files in os.walk(dir_name):
        for one_file in files:
            if one_file == file_name:
                return True, os.path.join(dir_name, one_file)
        for one_dir in dir_list:
            new_dir_name = os.path.join(dir_name, one_dir)
            result, result_file = get_file_by_name(new_dir_name, file_name)
            if result:
                return True, result_file
    return False, None


# 从 keystore 中获取私钥
def get_private_key_from_wallet_file(file_path, password):
    """从钱包文件中获取私钥(platon地址格式:bech32)

    Args:
        file_path: 钱包文件的全路径
        password: 钱包面

    Returns:
        str： 账户私钥
    """

    privateKey = ""
    with open(file_path) as keyfile:
        encrypted_key = keyfile.read()
        try:
            private_key = Account.decrypt(encrypted_key, password)
        except:
            raise Exception("The password is not correct, please enter the correct password")
        pri = keys.PrivateKey(private_key)
        privateKey = pri.to_hex()

    return privateKey


def queryContractBalanceInfo(platon, contract, from_address, contract_address, type="delegate",
                             fn_name="queryContractBalanceInfo"):
    data = contract.encodeABI(fn_name=fn_name)
    call_dict = {
        "from": from_address,
        "to": contract_address,
        "data": data,
    }

    result = platon.call(call_dict).hex()
    if result[0:2] == "0x":
        result = result[2:]

    if result == "":
        return ""
    # 数据长度
    data_len = int(result[64:128], 16)
    result = result[128:128 + data_len * 2]
    result = binascii.a2b_hex(result)
    ret_json = json.loads(result)
    # print(ret_json)
    if len(ret_json) > 0:
        if type == "delegate":
            encodeList = []
            investorFundAccountList = ret_json["investorFundAccountList"]
            for investorFundAccount in investorFundAccountList:
                investorFundAccount["address"] = encodeAddress(investorFundAccount["address"])
                encodeList.append(investorFundAccount)

            ret_json["investorFundAccountList"] = encodeList
        else:
            encodeFoundFundAccountList = []
            foundFundAccountList = ret_json["foundFundAccountList"]
            for foundFundAccount in foundFundAccountList:
                foundFundAccount["address"] = encodeAddress(foundFundAccount["address"])
                encodeFoundFundAccountList.append(foundFundAccount)
            ret_json["foundFundAccountList"] = encodeFoundFundAccountList

            encodeNodeFundAccountList = []
            nodeFundAccountList = ret_json["nodeFundAccountList"]
            for nodeFundAccount in nodeFundAccountList:
                nodeFundAccount["address"] = encodeAddress(nodeFundAccount["address"])
                encodeNodeFundAccountList.append(nodeFundAccount)
            ret_json["nodeFundAccountList"] = encodeNodeFundAccountList

    return ret_json


# 查询合约基础信息， 返回json
def queryContractInfo(platon, contract, from_address, contract_address, type="delegate",
                      fn_name="queryContractInfo"):
    data = contract.encodeABI(fn_name=fn_name)
    call_dict = {
        "from": from_address,
        "to": contract_address,
        "data": data,
    }
    try:
        result = platon.call(call_dict).hex()
        if result[0:2] == "0x":
            result = result[2:]

        if result == "":
            return ""

    except Exception as e:
        return ""
    # print(result)
    # 数据长度
    data_len = int(result[64:128], 16)
    result = result[128:128 + data_len * 2]
    result = binascii.a2b_hex(result)
    ret_json = json.loads(result)
    # print(ret_json)
    if len(ret_json) > 0:
        if type == "delegate":
            ret_json["investorOptAddr"] = encodeAddress(ret_json["investorOptAddr"])
            ret_json["investorIncomeAddr"] = encodeAddress(ret_json["investorIncomeAddr"])
            ret_json["trusteeOptAddr"] = encodeAddress(ret_json["trusteeOptAddr"])
            ret_json["trusteeIncomeAddr"] = encodeAddress(ret_json["trusteeIncomeAddr"])
            ret_json["trusteeSystemAddr"] = encodeAddress(ret_json["trusteeSystemAddr"])

            encodeList = []
            investorAddressList = ret_json["investorAddressList"]
            for investorAddress in investorAddressList:
                encodeList.append(encodeAddress(investorAddress))

            ret_json["investorAddressList"] = encodeList
        else:
            ret_json["foundOptAddr"] = encodeAddress(ret_json["foundOptAddr"])
            ret_json["foundIncomeAddr"] = encodeAddress(ret_json["foundIncomeAddr"])
            ret_json["foundSystemAddr"] = encodeAddress(ret_json["foundSystemAddr"])
            ret_json["nodeOptAddr"] = encodeAddress(ret_json["nodeOptAddr"])
            ret_json["nodeIncomeAddr"] = encodeAddress(ret_json["nodeIncomeAddr"])
            ret_json["nodeSystemAddr"] = encodeAddress(ret_json["nodeSystemAddr"])
            ret_json["benefitAddr"] = encodeAddress(ret_json["benefitAddr"])
            ret_json["stakingAddr"] = encodeAddress(ret_json["stakingAddr"])

            encodeFoundAddressList = []
            foundAddressList = ret_json["foundAddressList"]
            for foundAddress in foundAddressList:
                encodeFoundAddressList.append(encodeAddress(foundAddress))
            ret_json["foundAddressList"] = encodeFoundAddressList

            encodeNodeAddressList = []
            nodeAddressList = ret_json["nodeAddressList"]
            for nodeAddress in nodeAddressList:
                encodeNodeAddressList.append(encodeAddress(nodeAddress))
            ret_json["nodeAddressList"] = encodeNodeAddressList

    return ret_json


def sign_one_transaction_by_prikey(transaction, private_key):
    """
    签名交易
    :return:
        私钥
        交易数据
    """
    # 根据交易签名
    fields_transaction = filter_no_transaction_fields(transaction)
    one_transaction = transaction_str_to_int(fields_transaction)

    sign_data = Account.signTransaction(one_transaction, private_key, hrp_type)
    rawTransaction = copy(sign_data["rawTransaction"])
    strRawData = HexBytes(rawTransaction).hex()

    return strRawData, AttributeDict(sign_data, rawTransaction=strRawData)


def write_csv(file_name: str, dict_list: list):
    """将字典列表数据写进csv文件

    Args:
        file_name:  要写入的文件名称
        dict_list： 字典列表

    Raises:
        Exception： 写入文件不是以.csv为后缀，抛出异常
        :param file_name:
        :param dict_list:
    """
    if not file_name.endswith(".csv"):
        raise Exception("File format error")
    with open(file_name, "w", encoding="utf-8", newline='') as f:
        csv_write = csv.writer(f)
        csv_head = list(dict_list[0].keys())
        csv_write.writerow(csv_head)
        for one_dict in dict_list:
            csv_value = list(one_dict.values())
            csv_write.writerow(csv_value)


# 获取锁仓信息
def GetRestrictingInfo(platon, fromAddress, restrict_account):
    restrict_account = decodeBech32Address(restrict_account)
    data = rlp.encode([rlp.encode(int(4100)), rlp.encode(bytes.fromhex(restrict_account))])
    to_address = encodeAddress("0x1000000000000000000000000000000000000001")
    recive = platon.call({
        "from": fromAddress,
        "to": to_address,
        "data": data
    })
    recive = str(recive, encoding="ISO-8859-1")
    recive = json.loads(recive)
    code = int(recive['Code'])
    if code != 0:
        return None, None
    data = (recive["Ret"])
    amount = 0
    if data != "":
        data["balance"] = int(data["balance"], 16)
        data["Pledge"] = int(data["Pledge"], 16)
        data["debt"] = int(data["debt"], 16)
        if data["plans"]:
            for i in data["plans"]:
                i["amount"] = int(i["amount"], 16)
                amount = amount + i["amount"]
    # print(recive)
    return data, amount


# 处理锁仓交易/转账交易结果
def handleTxResult(res, checkFlag="log"):
    exitCode = 1
    if res == "" or res is None:
        return "交易失败！！！", exitCode

    if checkFlag == "log":
        code = ""
        if res.get("logs"):
            # print('res:{}'.format(res))
            logs = res.get("logs")
            # print('logs:{}'.format(logs))
            if len(logs) > 0:
                code = logs[0].get("data")
                # print('data:{}'.format(code))
            else:
                code = "out of gas"

        if code == '0xc130':
            result = "交易成功."
            exitCode = 0
        elif code == "":
            result = "请等待交易上链..."
            exitCode = 1
        else:
            result = "交易失败, 错误码是:" + code + ",请检查!!!"
            exitCode = 2
    elif checkFlag == "event":
        logs = []
        if res.get("logs"):
            logs = res.get("logs")
        if len(logs) > 0:
            return "交易成功.", 0
        else:
            return "交易失败！！！", exitCode
    else:
        status = res["status"]
        if status == 0:
            result = "交易失败, status:{} ".format(status)
        else:
            exitCode = 0
            result = "交易成功."

    return result, exitCode


def read_csv(file_name) -> list:
    """从csv文件中获取字典列表数据

    Args:
        file_name:  csv文件名称

    Returns:
        list：字典列表数据

    Raises:
        Exception： 写入文件不是以.csv为后缀，抛出异常
    """
    if not file_name.endswith(".csv"):
        raise Exception("File format error")
    transaction_list = []
    with open(file_name, encoding="utf-8") as csvfile:
        csv_reader = csv.reader(csvfile)
        header = next(csv_reader)
        for row in csv_reader:
            transaction_dict = dict(zip(header, row))
            transaction_list.append(transaction_dict)
    return transaction_list


# 计算合约地址
def calContractAddress(bech_address, nonce=0):
    _, address = decode("lat", bech_address)
    address = bytes(address).hex()
    data = rlp.encode([bytes.fromhex(address), nonce])

    new_account = Web3.sha3(data)[12:].hex()
    # print('new_account:{}'.format(new_account))

    if new_account[:2] == '0x':
        new_account = new_account[2:]

    new_account = list(bytes.fromhex(new_account))
    new_contract_address = encode("lat", new_account)
    # print('new_contract_address:{}'.format(new_contract_address))
    return new_contract_address


def get_time_stamp():
    '''
    获取时间戳
    :return:
    时间戳字符串：如：20200428092745170
    '''
    ct = time.time()
    local_time = time.localtime(ct)
    data_head = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    data_secs = (ct - int(ct)) * 1000
    time_stamp = "%s.%03d" % (data_head, data_secs)
    # print(time_stamp)
    stamp = ("".join(time_stamp.split()[0].split("-")) + "".join(time_stamp.split()[1].split(":"))).replace('.', '')
    return stamp


# 查询合约的call调用
def getCallValue(platon, contract, from_address, contract_address, fn_name, retType="int"):
    data = contract.encodeABI(fn_name=fn_name)
    call_dict = {
        "from": from_address,
        "to": contract_address,
        "data": data,
    }

    result = platon.call(call_dict)
    if retType == "int":
        if len(result) > 2:
            return int(result.hex(), 16)
        else:
            return 0
    elif retType == "uint256[4]":
        strRet = result.hex()
        if len(strRet) == 256 + 2:
            # 去掉0x
            strRet = strRet[2:]
            arrayList = [
                int(strRet[0:64], 16),
                int(strRet[64:128], 16),
                int(strRet[128:192], 16),
                int(strRet[192:256], 16)]
            return arrayList
        else:
            return [0, 0, 0, 0]
    else:
        return result.hex()


def filter_no_transaction_fields(transaction_dict: dict):
    """过滤非交易字段
    Args:
        transaction_dict:  交易字典

    Returns:
        dict：过滤非交易字段后的交易字典
    """
    new_transaction_dict = {}
    for key, value in transaction_dict.items():
        if key in ["from", "to", "data", "gas", "gasPrice", "value", "nonce", "chainId"]:
            new_transaction_dict[key] = value
    return new_transaction_dict


def transaction_str_to_int(transaction_dict: dict):
    """转换交易格式

    Args:
        transaction_dict:  交易字典

    Returns:
        dict：转换后的交易字典
    """
    for key, value in transaction_dict.items():
        if key in ["value", "gasPrice", "gas", "nonce", "chainId"]:
            transaction_dict[key] = int(value)
    return transaction_dict


def get_account_from_wallet_file(wallet_file, password):
    """从钱包文件中获取账户对象

    Args:
        wallet_file: 钱包文件绝对路径
        password： 钱包密码

    Returns:
        LocalAccount： 账户对象
    """
    with open(wallet_file) as keyfile:
        encrypted_key = keyfile.read()
        try:
            private_key = Account.decrypt(encrypted_key, password)
        except:
            raise Exception("The password is not correct, please enter the correct password")

        acc = Account.privateKeyToAccount(private_key, hrp_type)
    return acc, private_key


def getValueByColAndVal(data, desColName: str, desValue, selColName: str):
    """根据指定值和列名从xlsx返回值

        Args:
            desColName: 指定xlsx列名
            desValue: 指定值
            selColName: 查找值所在列名

        Returns:
        str： 查找到的值
    """
    # 获取指定值所在行号
    index = data[data[desColName].isin([desValue])].index.values[0]
    # print("index:{}".format(index))

    # 总账户地址
    info = data[selColName][index]
    # print("info:{}".format(info))
    return info


# 生成钱包内容
def generate_keyfile_json(password_bytes, key_bytes, hrp_type):
    # scrypt加密算法
    DKLEN = 32
    R = 8
    P = 1
    N = 16384

    salt = Random.get_random_bytes(32)
    derived_key = _scrypt_hash(
        password_bytes,
        salt=salt,
        buflen=DKLEN,
        r=R,
        p=P,
        n=N,
    )
    kdfparams = {
        'dklen': DKLEN,
        'n': N,
        'r': R,
        'p': P,
        'salt': encode_hex_no_prefix(salt),
    }

    iv = big_endian_to_int(Random.get_random_bytes(16))
    encrypt_key = derived_key[:16]
    ciphertext = encrypt_aes_ctr(key_bytes, encrypt_key, iv)
    mac = keccak(derived_key[16:32] + ciphertext)

    pub = keys.PrivateKey(key_bytes).public_key

    address = pub.to_bech32_address(hrp_type)
    return {
        'address': address,
        'crypto': {
            'cipher': 'aes-128-ctr',
            'cipherparams': {
                'iv': encode_hex_no_prefix(int_to_big_endian(iv)),
            },
            'ciphertext': encode_hex_no_prefix(ciphertext),
            'kdf': "scrypt",
            'kdfparams': kdfparams,
            'mac': encode_hex_no_prefix(mac),
        },
        'id': str(uuid.uuid4()),
        'version': 3,
    }


# 创建钱包
def create_v3_keyfile_json(password, hrp_type):
    extra_key_bytes = text_if_str(to_bytes, '')
    key_bytes = keccak(os.urandom(32) + extra_key_bytes)
    acct = Account.privateKeyToAccount(key_bytes)
    private_key = acct.privateKey
    pri = keys.PrivateKey(private_key).to_hex()

    if isinstance(pri, keys.PrivateKey):
        key_bytes = pri.to_bytes()
    else:
        key_bytes = HexBytes(pri)

    password_bytes = text_if_str(to_bytes, password)
    assert len(key_bytes) == 32

    encrypted = generate_keyfile_json(password_bytes, key_bytes, hrp_type)
    return acct, encrypted


def generate_map(file_name: str, data: str):
    """生成二维码图片

    Args:
        file_name: 二维码图片名称
        data: 二维码数据
    """
    img = qrcode.make(data)
    f = open(file_name, "wb")
    img.save(f)
    f.close()


def getAddessListByStr(strAddress):
    if strAddress == "" or strAddress is None or pd.isna(strAddress):
        return []
    strAddressList = strAddress.split("\n")

    return strAddressList
