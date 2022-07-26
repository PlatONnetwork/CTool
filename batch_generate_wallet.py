import os
import sys
import click
import pandas as pd
from crypto import HDPrivateKey, HDKey
import io
import shutil
import json

from platon_utility import generate_map, hrp_type, wallet_file_base_dir, prikey_file_base_dir
from random import choice
import string


def generate_password(length=8, chars=string.ascii_letters + string.digits):
    return ''.join([choice(chars) for i in range(length)])


@click.command(help='批量生成钱包文件')
@click.option('-f', '--filepath', 'filePath', required=True, help='钱包配置文件.')
@click.option('-t', '--type', 'type', required=True, default="ordinary",
              help='生成钱包类型：ordinary:普通钱包, hd:分层钱包',
              type=click.Choice(['ordinary', 'hd']))
@click.option('-n', '--net_type', 'net_type', required=False,
              help='网络类型：[alaya]:Alaya网络, [platon]:PlatON网络',
              type=click.Choice(['alaya', 'platon']))
@click.option('--empty_keystore/--no-empty_keystore', default=False, help='是否清空保存钱包文件的目录.')
def batch_generate_wallet(filePath, type, empty_keystore, net_type):
    if not os.path.exists(filePath):
        print("钱包配置文件不存在,请检查: {}".format(filePath))
        sys.exit(1)

    if "alaya" == net_type:
        pri_path = "m/44'/206'/0'"
    else:
        pri_path = "m/44'/486'/0'"

    if empty_keystore:
        try:
            shutil.rmtree(wallet_file_base_dir)
            shutil.rmtree(prikey_file_base_dir)
        except FileNotFoundError:
            pass

    if type == "ordinary":
        # 生成普通钱包
        ord_wallet = pd.read_excel(filePath, sheet_name='生成普通钱包')
        # 记录相同：私钥管理人/账户用途/钱包类型
        dict_same_account_use = {}
        for index, row in ord_wallet.iterrows():
            prikey_manager = str(row['私钥管理人'])
            if prikey_manager == "" or prikey_manager == "nan":
                continue
            account_use = row['账户用途']
            wallet_type = row['钱包类型']
            if "普通钱包" != wallet_type:
                print("私钥管理人:{}, 账户用途:{}, 钱包类型错误：{}".format(prikey_manager, account_use, wallet_type))
                sys.exit(1)

            address_count = int(row['地址数量'])

            account_path = "{0}/{1}/{2}".format(prikey_manager, wallet_type, account_use)
            if account_path in dict_same_account_use:
                current_group = dict_same_account_use.get(account_path)
                current_group += 1
            else:
                # 记录第0组
                current_group = 0
            dict_same_account_use[account_path] = current_group

            # 创建目录
            new_wallet_dir = "{0}/{1}/{2}/{3}/{4}". \
                format(wallet_file_base_dir, prikey_manager, wallet_type, account_use, current_group)

            new_prikey_dir = "{0}/{1}/{2}/{3}/{4}". \
                format(prikey_file_base_dir, prikey_manager, wallet_type, account_use, current_group)

            if not os.path.exists(new_wallet_dir):
                os.makedirs(new_wallet_dir)

            if not os.path.exists(new_prikey_dir):
                os.makedirs(new_prikey_dir)

            # 随机生成密码，并保存密码文件
            password = generate_password()

            # 生成钱包私钥文件（助记词）
            # 生成私钥和地址
            cols = ['私钥管理人', '账户用途', '钱包类型', '账户分组', '账户序号', '助记词']
            mnemonic_df = pd.read_csv(io.StringIO(""), names=cols,
                                      dtype=dict(zip(cols, [str, str, str, int, int, str])),
                                      index_col=['私钥管理人', '账户用途', '钱包类型', '账户分组', '账户序号'])

            # 统计地址列表
            cols = ['私钥管理人', '账户用途', '钱包类型', '账户分组', '账户序号', '账户地址']
            addresses_df = pd.read_csv(io.StringIO(""), names=cols,
                                       dtype=dict(zip(cols, [str, str, str, int, int, str])),
                                       index_col=['私钥管理人', '账户用途', '钱包类型', '账户分组', '账户序号'])

            keystores = {}
            privateKeys = {}
            for i in range(address_count):
                # 生成助记
                master_key, mnemonic = HDPrivateKey.master_key_from_entropy()
                mnemonic_df.loc[(prikey_manager, account_use, wallet_type, current_group, i), :] = [mnemonic]
                print('生成:{0}/{1}-{2}地址'.format(account_path, current_group, i))

                root_keys = HDKey.from_path(master_key, pri_path)
                acct_priv_key = root_keys[-1]
                # 钱包
                keys = HDKey.from_path(acct_priv_key, '{change}/{index}'.format(change=0, index=0))
                private_key = keys[-1]
                public_address = private_key.public_key.address(hrp_type)
                addresses_df.loc[(prikey_manager, account_use, wallet_type, current_group, i), :] = [public_address]
                keystores[public_address] = json.dumps(private_key._key.to_keyfile_json(password, hrp_type))
                privateKeys[public_address] = private_key._key.get_private_key()

            private_key_file_name = "private_key_{}_{}_{}.xlsx".format(prikey_manager, account_use, current_group)
            private_key_file = "{0}/{1}".format(new_wallet_dir, private_key_file_name)
            print('生成账户私钥文件:{}'.format(private_key_file))
            writer = pd.ExcelWriter(private_key_file, engine='xlsxwriter')
            mnemonic_df.to_excel(writer, sheet_name='账户私钥',
                                 index=['私钥管理人', '账户用途', '钱包类型', '账户分组', '账户序号'],
                                 merge_cells=False)
            # 设置特定单元格的宽度
            worksheets = writer.sheets
            worksheets['账户私钥'].set_column(0, 0, 20)
            worksheets['账户私钥'].set_column(1, 1, 15)
            worksheets['账户私钥'].set_column(2, 2, 10)
            worksheets['账户私钥'].set_column(3, 3, 10)
            worksheets['账户私钥'].set_column(4, 4, 10)
            worksheets['账户私钥'].set_column(5, 5, 90)
            writer.save()
            writer.close()

            address_file_name = "address_{}_{}_{}.xlsx".format(prikey_manager, account_use, current_group)
            address_list_file = "{0}/{1}".format(new_wallet_dir, address_file_name)
            print('生成账户地址列表文件:{0}'.format(address_list_file))
            writer = pd.ExcelWriter(address_list_file, engine='xlsxwriter')
            addresses_df.to_excel(writer, sheet_name='账户地址',
                                  index=['私钥管理人', '账户用途', '钱包类型', '账户分组', '账户序号'],
                                  merge_cells=False)
            # 设置特定单元格的宽度
            worksheets = writer.sheets
            worksheets['账户地址'].set_column(0, 0, 20)
            worksheets['账户地址'].set_column(1, 1, 15)
            worksheets['账户地址'].set_column(2, 2, 10)
            worksheets['账户地址'].set_column(3, 3, 10)
            worksheets['账户地址'].set_column(4, 4, 10)
            worksheets['账户地址'].set_column(5, 5, 70)
            writer.save()
            writer.close()

            # 生成钱包文件/钱包私钥文件
            print('生成钱包文件:{0}'.format(new_wallet_dir))
            password_file_name = "password_{}_{}_{}.txt".format(prikey_manager, account_use, current_group)
            password_file = os.path.join(new_wallet_dir, password_file_name)
            with open(password_file, 'a+') as pf:
                for index, row in addresses_df.iterrows():
                    strPassInfo = row['账户地址'] + ":" + password
                    pf.write(strPassInfo + '\n')

                    fn = "{0}.json".format(row['账户地址'])
                    wallet_file_path = "{0}/{1}".format(new_wallet_dir, fn)
                    with open(wallet_file_path, "w") as f:
                        f.write(keystores[row['账户地址']])

                    # 生成二维码图片
                    image_name = "{0}.png".format(row['账户地址'])
                    prikey_file_path = "{0}/{1}".format(new_prikey_dir, image_name)
                    generate_map(prikey_file_path, privateKeys[row['账户地址']])
    else:
        # 生成HD钱包
        hd_wallet = pd.read_excel(filePath, sheet_name='生成HD钱包')

        for index, row in hd_wallet.iterrows():
            prikey_manager = str(row['私钥管理人'])
            if prikey_manager == "" or prikey_manager == "nan":
                continue
            account_use = row['账户用途']
            wallet_type = row['钱包类型']
            if "HD钱包" != wallet_type:
                print("私钥管理人:{}, 账户用途:{}, 钱包类型错误：{}".format(prikey_manager, account_use, wallet_type))
                sys.exit(1)

            address_group_count = int(row['地址组数'])
            wallet_count = int(row['每组钱包数量'])

            for i in range(address_group_count):
                # 创建目录
                new_wallet_dir = "{0}/{1}/{2}/{3}/{4}". \
                    format(wallet_file_base_dir, prikey_manager, wallet_type, account_use, i)

                new_prikey_dir = "{0}/{1}/{2}/{3}/{4}". \
                    format(prikey_file_base_dir, prikey_manager, wallet_type, account_use, i)

                os.makedirs(new_wallet_dir)
                os.makedirs(new_prikey_dir)

                # 随机生成密码，并保存密码文件
                password = generate_password()

                # 生成钱包私钥文件（助记词）
                # 生成私钥和地址
                cols = ['私钥管理人', '账户用途', '钱包类型', '账户分组', '账户序号', '助记词']
                mnemonic_df = pd.read_csv(io.StringIO(""), names=cols,
                                          dtype=dict(zip(cols, [str, str, str, int, int, str])),
                                          index_col=['私钥管理人', '账户用途', '钱包类型', '账户分组', '账户序号'])

                # 统计地址列表
                cols = ['私钥管理人', '账户用途', '钱包类型', '账户分组', '账户序号', '账户地址']
                addresses_df = pd.read_csv(io.StringIO(""), names=cols,
                                           dtype=dict(zip(cols, [str, str, str, int, int, str])),
                                           index_col=['私钥管理人', '账户用途', '钱包类型', '账户分组', '账户序号'])

                print('生成【{0}/{1}/{2}】第{3}组助记词和地址==========='.format(
                    prikey_manager, wallet_type, account_use, i))

                keystores = {}
                privateKeys = {}
                # 生成助记词
                master_key, mnemonic = HDPrivateKey.master_key_from_entropy()
                mnemonic_df.loc[(prikey_manager, account_use, wallet_type, i, 0), :] = [mnemonic]

                root_keys = HDKey.from_path(master_key, pri_path)
                acct_priv_key = root_keys[-1]

                # HD钱包
                for j in range(wallet_count):
                    keys = HDKey.from_path(acct_priv_key, '{change}/{index}'.format(change=0, index=j))
                    private_key = keys[-1]
                    public_address = private_key.public_key.address(hrp_type)
                    keystores[public_address] = json.dumps(private_key._key.to_keyfile_json(password, hrp_type))
                    privateKeys[public_address] = private_key._key.get_private_key()
                    addresses_df.loc[(prikey_manager, account_use, wallet_type, i, j), :] = [public_address]

                private_key_file_name = "private_key_{}_{}_{}.xlsx".format(prikey_manager, account_use, i)
                private_key_file = "{0}/{1}".format(new_wallet_dir, private_key_file_name)
                print('生成账户私钥文件:{}'.format(private_key_file))
                writer = pd.ExcelWriter(private_key_file, engine='xlsxwriter')
                mnemonic_df.to_excel(writer, sheet_name='账户私钥',
                                     index=['私钥管理人', '账户用途', '钱包类型', '账户分组', '账户序号'],
                                     merge_cells=False)
                # 设置特定单元格的宽度
                worksheets = writer.sheets
                worksheets['账户私钥'].set_column(0, 0, 20)
                worksheets['账户私钥'].set_column(1, 1, 15)
                worksheets['账户私钥'].set_column(2, 2, 10)
                worksheets['账户私钥'].set_column(3, 3, 10)
                worksheets['账户私钥'].set_column(4, 4, 10)
                worksheets['账户私钥'].set_column(5, 5, 90)
                writer.save()
                writer.close()

                address_file_name = "address_{}_{}_{}.xlsx".format(prikey_manager, account_use, i)
                address_list_file = "{0}/{1}".format(new_wallet_dir, address_file_name)
                print('生成账户地址列表文件:{0}'.format(address_list_file))
                writer = pd.ExcelWriter(address_list_file, engine='xlsxwriter')
                addresses_df.to_excel(writer, sheet_name='账户地址',
                                      index=['私钥管理人', '账户用途', '钱包类型', '账户分组', '账户序号'],
                                      merge_cells=False)
                # 设置特定单元格的宽度
                worksheets = writer.sheets
                worksheets['账户地址'].set_column(0, 0, 20)
                worksheets['账户地址'].set_column(1, 1, 15)
                worksheets['账户地址'].set_column(2, 2, 10)
                worksheets['账户地址'].set_column(3, 3, 10)
                worksheets['账户地址'].set_column(4, 4, 10)
                worksheets['账户地址'].set_column(5, 5, 70)
                writer.save()
                writer.close()

                # 生成钱包文件/钱包私钥文件
                print('生成钱包文件:{0}'.format(new_wallet_dir))
                password_file_name = "password_{}_{}_{}.txt".format(prikey_manager, account_use, i)
                password_file = os.path.join(new_wallet_dir, password_file_name)
                with open(password_file, 'a+') as pf:
                    for index, row in addresses_df.iterrows():
                        strPassInfo = row['账户地址'] + ":" + password
                        pf.write(strPassInfo + '\n')

                        fn = "{0}.json".format(row['账户地址'])
                        wallet_file_path = "{0}/{1}".format(new_wallet_dir, fn)
                        with open(wallet_file_path, "w") as f:
                            f.write(keystores[row['账户地址']])

                        # 生成二维码图片
                        image_name = "{0}.png".format(row['账户地址'])
                        prikey_file_path = "{0}/{1}".format(new_prikey_dir, image_name)
                        generate_map(prikey_file_path, privateKeys[row['账户地址']])

    print("生成钱包完成.")


if __name__ == '__main__':
    batch_generate_wallet()
