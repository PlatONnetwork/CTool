import json
import os
import sys
import click
import pandas as pd
from platon_utility import get_abi_func_inputs_by_name
import io


@click.command(help='生成erc20合约配置文件')
@click.option('-a', '--abi_path', required=True, help='合约abi所在文件路径.')
@click.option('-c', '--contract_name', 'contractName', required=False, default="ERC20", help='合约名称.')
@click.option('-n', '--function_name', 'func_name', required=False,
              default="constructor", help='函数名, constructor为部署合约.')
@click.option('-s', '--save_dir', 'saveDir', required=False, help='保存配置文件路径.')
def generate_erc20_file(abi_path, contractName, func_name, saveDir):
    try:
        if not os.path.exists(abi_path):
            print("合约abi文件不存在,请检查: {}".format(abi_path))
            sys.exit(1)

        if not saveDir:
            saveDir = os.getcwd()
        elif saveDir and not os.path.exists(saveDir):
            print("保存目录不存在,请检查: {}".format(saveDir))
            sys.exit(1)

        erc20_abi = json.load(open(abi_path))
        func_inputs_list = get_abi_func_inputs_by_name(erc20_abi, func_name)

        cols = ['from', 'contract_address']
        type_cols = [str, str]
        for input_info in func_inputs_list:
            field_name = input_info["field_name"]
            cols.append(field_name)
            type_cols.append(str)

        # 生成erc20合约配置文件
        erc20_df = pd.read_csv(io.StringIO(""), names=cols,
                                       dtype=dict(zip(cols, type_cols)))

        file_name = "{}_{}_file.xlsx".format(contractName, func_name)
        save_file_path = os.path.join(saveDir, file_name)
        writer = pd.ExcelWriter(save_file_path, engine='xlsxwriter')
        sheet_name = "{}_{}".format(contractName, func_name)
        erc20_df.to_excel(writer, sheet_name=sheet_name, index=None)
        # 设置特定单元格的宽度
        worksheets = writer.sheets
        worksheets[sheet_name].set_column(0, 0, 45)
        worksheets[sheet_name].set_column(1, 1, 45)
        for i in range(2, len(func_inputs_list)+2):
            worksheets[sheet_name].set_column(i, i, 20)

        writer.save()
        writer.close()
    except Exception as e:
        print('{} {}'.format('exception: ', e))
        print('生成erc20合约:{} {} 配置文件失败!!!'.format(contractName, func_name, save_file_path))
        sys.exit(1)

    else:
        print("生成erc20合约:{} {} 配置文件完成：{}.".format(contractName, func_name, save_file_path))


if __name__ == '__main__':
    generate_erc20_file()
